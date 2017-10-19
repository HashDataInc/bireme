/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

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

  private Long lastMergeTime;
  private int mergeInterval;
  private int batchSize;

  public LinkedBlockingQueue<Row> rows;
  private LinkedBlockingQueue<CommitCallback> commitCallback;

  private LinkedList<RowBatchMerger> localMerger;
  private LinkedBlockingQueue<Future<LoadTask>> taskIn;

  /**
   * Create cache for a destination table.
   *
   * @param cxt The bireme context.
   * @param tableName The table name to cached.
   */
  public RowCache(Context cxt, String tableName) {
    this.cxt = cxt;

    this.lastMergeTime = new Date().getTime();
    this.mergeInterval = cxt.conf.merge_interval;
    this.batchSize = cxt.conf.batch_size;

    this.rows = new LinkedBlockingQueue<Row>(cxt.conf.batch_size * 2);
    this.commitCallback = new LinkedBlockingQueue<CommitCallback>();

    ChangeLoader loader = cxt.changeLoaders.get(tableName);
    this.taskIn = loader.taskIn;

    this.localMerger = new LinkedList<RowBatchMerger>();
    for (int i = 0; i < cxt.conf.loader_task_queue_size; i++) {
      localMerger.add(new RowBatchMerger());
    }
  }

  /**
   * Add an Array of {@code Rows} to cache one by one. This method is continuously blocking for a
   * while when there is no available space in the cache.
   *
   * @param newRows the array of {@code Rows}
   * @param callback the corresponding {@code CommitCallback}
   * @throws BiremeException Exception while borrow from pool
   * @throws InterruptedException if interrupted while waiting
   */
  public boolean addRows(ArrayList<Row> newRows, CommitCallback callback) throws BiremeException {
    boolean success = false;

    synchronized (rows) {
      if (rows.remainingCapacity() < newRows.size()) {
        return success;
      }

      success = rows.addAll(newRows) && commitCallback.offer(callback);
    }

    return success;
  }

  public boolean shouldMerge() {
    if (new Date().getTime() - lastMergeTime < mergeInterval && rows.size() < batchSize) {
      return false;
    }
    return true;
  }

  public void startMerge() throws BiremeException {
    synchronized (taskIn) {
      if (taskIn.remainingCapacity() == 0 || rows.isEmpty()) {
        return;
      }

      ArrayList<CommitCallback> callbacks = new ArrayList<CommitCallback>();
      ArrayList<Row> batch = null;

      try {
        batch = cxt.idleRowArrays.borrowObject();
      } catch (Exception e) {
        String message = "Can't not borrow RowArrays from the Object Pool.\n";
        throw new BiremeException(message, e);
      }

      synchronized (rows) {
        rows.drainTo(batch);
        commitCallback.drainTo(callbacks);
      }

      RowBatchMerger merger = localMerger.remove();
      merger.setBatch(batch, callbacks);

      ExecutorService mergerPool = cxt.mergerPool;
      Future<LoadTask> task = mergerPool.submit(merger);
      taskIn.add(task);
      localMerger.add(merger);
    }
  }

  /**
   * {@code RowBatchMerger} accepts a batch of {@code Rows}, merge them and create a
   * {@code LoadTask}.
   *
   * @author yuze
   *
   */
  public class RowBatchMerger implements Callable<LoadTask> {
    protected ArrayList<Row> rows;
    protected ArrayList<CommitCallback> callbacks;

    /**
     * Create a RowBatchMerger.
     *
     * @param cxt The bireme context
     * @param mappedTableName the destination table
     * @param rows batch of {@code Rows} to be merged
     * @param callbacks the {@code CommitCallbacks} in this batch.
     */
    public RowBatchMerger() {}

    public void setBatch(ArrayList<Row> rows, ArrayList<CommitCallback> callbacks) {
      this.rows = rows;
      this.callbacks = callbacks;
    }

    /**
     * Run the {@code RowBatchMerger}.
     */
    public LoadTask call() {
      LoadTask task = new LoadTask();
      LoadState state = task.loadState;

      for (Row row : rows) {
        state.setProduceTime(row.originTable, row.produceTime);
        state.setReceiveTime(row.originTable, row.receiveTime);

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

      cxt.idleRowArrays.returnObject(rows);

      task.callbacks = callbacks;

      return task;
    }
  }
}
