package cn.hashdata.dbsync;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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

  public Long lastTaskTime;
  public LinkedBlockingQueue<Row> rows;
  public Context cxt;

  /**
   * Create cache for a destination table.
   *
   * @param rowSize The maximum can be cached.
   * @param cxt The dbsync context.
   */
  public RowCache(int rowSize, Context cxt) {
    this.cxt = cxt;
    rows = new LinkedBlockingQueue<Row>(rowSize);
    lastTaskTime = new Date().getTime();
  }

  /**
   * Add an Array of {@code Rows} to cache one by one. This method is continuously blocking for a
   * while when there is no available space in the cache.
   *
   * @param rows the Array of {@code Rows}
   * @throws InterruptedException - if interrupted while waiting
   */
  public void addRows(ArrayList<Row> rows) throws InterruptedException {
    boolean success;
    for (Row row : rows) {
      do {
        success = this.rows.offer(row, TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } while (!success && !cxt.stop);

      if (cxt.stop) {
        break;
      }
    }
  }

  /**
   * Drain the cached {@code Rows} and return a batch of {@code Rows}. This method is executed once
   * in a while or the cached Rows reach to a certain amount.
   *
   * @param interval The minimum time between two operations
   * @param maxSize The amount condition to trigger the operation
   * @return the drained array of {@code Rows}
   * @throws InterruptedException - if interrupted while waiting
   * @throws Exception Exception while borrow from pool
   */
  public ArrayList<Row> createBatch(long interval, long maxSize)
      throws InterruptedException, Exception {
    if (new Date().getTime() - lastTaskTime < interval && rows.size() < maxSize) {
      return null;
    }

    if (rows.isEmpty()) {
      return null;
    }

    ArrayList<Row> batch = cxt.idleRowArrays.borrowObject();
    rows.drainTo(batch, (int) maxSize);
    lastTaskTime = new Date().getTime();
    return batch;
  }

  /**
   * {@code RowBatchMerger} accepts a batch of {@code Rows}, merge them and create a
   * {@code LoadTask}.
   *
   * @author yuze
   *
   */
  public static class RowBatchMerger implements Callable<LoadTask> {
    protected String mappedTableName;
    protected ArrayList<Row> rows;
    protected Context cxt;

    private Logger logger = LogManager.getLogger("Dbsync." + RowBatchMerger.class);

    /**
     * Create a RowBatchMerger.
     *
     * @param mappedTableName The destination table
     * @param rows The batch of {@code Rows} to be merged
     * @param cxt The dbsync context
     */
    public RowBatchMerger(String mappedTableName, ArrayList<Row> rows, Context cxt) {
      this.mappedTableName = mappedTableName;
      this.rows = rows;
      this.cxt = cxt;
    }

    /**
     * Run the {@code RowBatchMerger}.
     */
    public LoadTask call() throws InterruptedException {
      Thread.currentThread().setName("RowBatchMerger");

      LoadTask task = new LoadTask(mappedTableName);
      HashMap<String, Position> position = task.positions;

      for (Row row : rows) {
        // update position
        position.put(row.originTable, row.position);

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

      if (task.insert.isEmpty() && task.delete.isEmpty()) {
        return null;
      }

      return task;
    }
  }
}
