/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import cn.hashdata.bireme.provider.PipeLine;
import cn.hashdata.bireme.provider.PipeLine.PipeLineState;

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
  public PipeLine pipeLine;

  private Long lastMergeTime;
  private int mergeInterval;
  private int batchSize;

  public LinkedBlockingQueue<Row> rows;
  private LinkedBlockingQueue<CommitCallback> commitCallback;

  private LinkedList<RowBatchMerger> localMerger;
  public LinkedBlockingQueue<Future<LoadTask>> taskOut;
  public ChangeLoader loader;
  public Future<Long> loadResult;

  /**
   * Create cache for a destination table.
   *
   * @param cxt The bireme context.
   * @param tableName The table name to cached.
   */
  public RowCache(Context cxt, String tableName, PipeLine pipeLine) {
    this.cxt = cxt;
    this.pipeLine = pipeLine;

    this.lastMergeTime = new Date().getTime();
    this.mergeInterval = cxt.conf.merge_interval;
    this.batchSize = cxt.conf.batch_size;

    this.rows = new LinkedBlockingQueue<Row>(cxt.conf.batch_size * 2);
    this.commitCallback = new LinkedBlockingQueue<CommitCallback>();

    this.localMerger = new LinkedList<RowBatchMerger>();
    for (int i = 0; i < cxt.conf.loader_task_queue_size; i++) {
      localMerger.add(new RowBatchMerger());
    }

    this.taskOut = new LinkedBlockingQueue<Future<LoadTask>>(cxt.conf.loader_task_queue_size);
    this.loader = new ChangeLoader(cxt, tableName, taskOut);
  }

  /**
   * Add an Array of {@code Rows} to cache one by one. This method is continuously blocking for a
   * while when there is no available space in the cache.
   *
   * @param newRows the array of {@code Rows}
   * @param callback the corresponding {@code CommitCallback}
   * @throws BiremeException when cache has enough but cannot add to cache
   */
  public boolean addRows(ArrayList<Row> newRows, CommitCallback callback) {
    synchronized (rows) {
      if (rows.remainingCapacity() < newRows.size()) {
        return false;
      }

      rows.addAll(newRows);
      commitCallback.offer(callback);
    }

    return true;
  }

  public boolean shouldMerge() {
    if (new Date().getTime() - lastMergeTime < mergeInterval && rows.size() < batchSize) {
      return false;
    }
    return true;
  }

  public void startMerge() throws BiremeException {
    synchronized (taskOut) {
      if (taskOut.remainingCapacity() == 0 || rows.isEmpty()) {
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
      taskOut.add(task);
      localMerger.add(merger);
    }
  }

  public void startLoad() {
    Future<LoadTask> head = taskOut.peek();
    if (head != null && head.isDone()) {
      // get result of last load
      if (loadResult != null && loadResult.isDone()) {
        try {
          loadResult.get();
        } catch (ExecutionException e) {
          pipeLine.state = PipeLineState.ERROR;
          pipeLine.e = new BiremeException("Loader failed.\n", e.getCause());
          return;
        } catch (InterruptedException e) {
          pipeLine.state = PipeLineState.ERROR;
          pipeLine.e = new BiremeException("Get Future<Long> failed, be interrupted", e);
          return;
        }
      }

      // start a new load
      if (loadResult == null || loadResult.isDone()) {
        loadResult = cxt.loaderPool.submit(loader);
      }
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
