package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An in-memory cache for {@code Row}. We use cache to merge and load operations in batch.
 * {@code Rows} are separated by its destination table. In each cache, {@code Rows} are ordered.
 *
 * @author yuze
 *
 */
public class RowCache {
  static final protected Long TIMEOUT_MS = 1000L;

  public Context cxt;
  public Long lastTaskTime;
  public String tableName;
  public LinkedBlockingQueue<Row> rows;
  public LinkedBlockingQueue<CommitCallback> commitCallback;
  public LinkedBlockingQueue<RowBatchMerger> rowBatchMergers;
  public int mergeInterval;
  public int batchSize;

  /**
   * Create cache for a destination table.
   *
   * @param cxt The bireme context.
   * @param tableName The table name to cached.
   */
  public RowCache(Context cxt, String tableName) {
    this.cxt = cxt;
    this.tableName = tableName;
    this.mergeInterval = cxt.conf.merge_interval;
    this.batchSize = cxt.conf.batch_size;
    lastTaskTime = new Date().getTime();

    this.rows = new LinkedBlockingQueue<Row>(cxt.conf.batch_size * 2);
    this.commitCallback = new LinkedBlockingQueue<CommitCallback>();
    this.rowBatchMergers =
        new LinkedBlockingQueue<RowBatchMerger>(cxt.conf.row_cache_size / cxt.conf.batch_size);
  }

  /**
   * Add an Array of {@code Rows} to cache one by one. This method is continuously blocking for a
   * while when there is no available space in the cache.
   *
   * @param newRows the array of {@code Rows}
   * @param callback the corresponding {@code CommitCallback}
   * @throws InterruptedException if interrupted while waiting
   * @throws BiremeException Exception while borrow from pool
   */
  public void addRows(ArrayList<Row> newRows, CommitCallback callback)
      throws InterruptedException, BiremeException {
    createBatch();

    for (Row row : newRows) {
      boolean success;

      do {
        success = rows.offer(row, TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } while (!success && !cxt.stop);

      if (cxt.stop) {
        break;
      }
    }
    commitCallback.offer(callback);

    createBatch();
  }

  /**
   * Drain the cached {@code Rows} and return a batch of {@code Rows}. This method is executed once
   * in a while or the cached Rows reach to a certain amount.
   *
   * @param interval The minimum time between two operations
   * @param maxSize The amount condition to trigger the operation
   * @return the drained array of {@code Rows}
   * @throws InterruptedException if interrupted while waiting
   * @throws BiremeException Exception while borrow from pool
   */
  private synchronized void createBatch() throws InterruptedException, BiremeException {
    if (new Date().getTime() - lastTaskTime < mergeInterval && rows.size() < batchSize) {
      return;
    }

    if (rows.isEmpty()) {
      return;
    }

    ArrayList<Row> rows = null;

    try {
      rows = cxt.idleRowArrays.borrowObject();
    } catch (Exception e) {
      throw new BiremeException(e);
    }

    this.rows.drainTo(rows);
    ArrayList<CommitCallback> callbacks = new ArrayList<CommitCallback>();
    this.commitCallback.drainTo(callbacks);
    RowBatchMerger batch = new RowBatchMerger(cxt, tableName, rows, callbacks);

    boolean success;

    do {
      success = rowBatchMergers.offer(batch, TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } while (!success && !cxt.stop);

    lastTaskTime = new Date().getTime();
  }

  /**
   * Fetch a batch of rows from cache
   *
   * @return the batch
   * @throws InterruptedException if interrupted while waiting
   * @throws BiremeException - Exception while borrow from pool
   */
  public RowBatchMerger fetchBatch() throws InterruptedException, BiremeException {
    createBatch();

    RowBatchMerger batch = null;

    do {
      batch = rowBatchMergers.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } while (batch == null && !cxt.stop);

    return batch;
  }

  /**
   * {@code RowBatchMerger} accepts a batch of {@code Rows}, merge them and create a
   * {@code LoadTask}.
   *
   * @author yuze
   *
   */
  public class RowBatchMerger implements Callable<LoadTask> {
    protected String mappedTableName;
    protected ArrayList<Row> rows;
    protected ArrayList<CommitCallback> callbacks;
    protected Context cxt;

    private Logger logger = LogManager.getLogger("Bireme." + RowBatchMerger.class);

    /**
     * Create a RowBatchMerger.
     *
     * @param cxt The bireme context
     * @param mappedTableName the destination table
     * @param rows  batch of {@code Rows} to be merged
     * @param callbacks the {@code CommitCallbacks} in this batch.
     */
    public RowBatchMerger(Context cxt, String mappedTableName, ArrayList<Row> rows,
        ArrayList<CommitCallback> callbacks) {
      this.cxt = cxt;
      this.mappedTableName = mappedTableName;
      this.rows = rows;
      this.callbacks = callbacks;
    }

    /**
     * Run the {@code RowBatchMerger}.
     */
    public LoadTask call() throws InterruptedException {
      Thread.currentThread().setName("RowBatchMerger");

      LoadTask task = new LoadTask(mappedTableName);

      for (Row row : rows) {
        switch (row.type) {
          case INSERT:
            task.insert.put(row.keys, row.tuple);
            break;
          case DELETE:
            if (task.insert.containsKey(row.keys)) {
              task.insert.remove(row.keys);
            }

            task.delete.add(row.keys);
            break;
          case UPDATE:
            if (row.oldKeys != null) {
              if (task.insert.containsKey(row.oldKeys)) {
                task.insert.remove(row.oldKeys);
              }

              task.delete.add(row.oldKeys);
              task.insert.put(row.keys, row.tuple);
            } else {
              task.delete.add(row.keys);
              task.insert.put(row.keys, row.tuple);
            }
        }

        cxt.idleRows.returnObject(row);
      }

      logger.trace("Merge batch {} (size: {}) to task {} (including {} inserts and {} delete.)",
          rows.hashCode(), rows.size(), task.hashCode(), task.insert.size(), task.delete.size());

      cxt.idleRowArrays.returnObject(rows);

      task.callbacks = callbacks;
      return task;
    }
  }
}
