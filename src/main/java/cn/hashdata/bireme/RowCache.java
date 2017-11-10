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

import cn.hashdata.bireme.pipeline.PipeLine;

/**
 * An in-memory cache for {@link Row}. We use cache to merge and load change data in batch.
 * {@code Rows} are separated by its destination table. In each cache, {@code Rows} are ordered.
 *
 * @author yuze
 *
 */
public class RowCache {
  static final protected Long TIMEOUT_MS = 1000L;

  public Context cxt;
  public String tableName;
  public PipeLine pipeLine;

  private Long lastMergeTime;
  private int mergeInterval;
  private int batchSize;

  public LinkedBlockingQueue<Row> rows;
  private LinkedBlockingQueue<CommitCallback> commitCallback;

  private LinkedList<RowBatchMerger> localMerger;
  public LinkedBlockingQueue<Future<LoadTask>> mergeResult;
  public ChangeLoader loader;
  public Future<Long> loadResult;

  /**
   * Create cache for a destination table.
   *
   * @param cxt the bireme context
   * @param tableName the table name to cached
   * @param pipeLine the pipeLine belongs to which
   */
  public RowCache(Context cxt, String tableName, PipeLine pipeLine) {
    this.cxt = cxt;
    this.tableName = tableName;
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

    this.mergeResult = new LinkedBlockingQueue<Future<LoadTask>>(cxt.conf.loader_task_queue_size);
    this.loader = new ChangeLoader(cxt, pipeLine, tableName, mergeResult);

    // add statistics
    pipeLine.stat.addGaugeForCache(tableName, this);
  }

  /**
   * Add an Array of {@code Rows} to cache one by one. This method is continuously blocking for a
   * while when there is no available space in the cache.
   *
   * @param newRows the array of {@code Rows}
   * @param callback callback the corresponding {@code CommitCallback}
   * @return true if successfully add to cache
   */
  public boolean addRows(ArrayList<Row> newRows, CommitCallback callback) {
    if (rows.remainingCapacity() < newRows.size()) {
      return false;
    }

    rows.addAll(newRows);
    commitCallback.offer(callback);

    return true;
  }

  /**
   * Whether reach the threshold to merge.
   *
   * @return true if reach the threshold to merge
   */
  public boolean shouldMerge() {
    if (new Date().getTime() - lastMergeTime < mergeInterval && rows.size() < batchSize) {
      return false;
    }
    return true;
  }

  /**
   * Drain current {@code Row} in cache and allocate a {@code RowbatchMerget} to merge.
   */
  public void startMerge() {
    if (mergeResult.remainingCapacity() == 0 || rows.isEmpty()) {
      return;
    }

    ArrayList<CommitCallback> callbacks = new ArrayList<CommitCallback>();
    ArrayList<Row> batch = new ArrayList<Row>();

    rows.drainTo(batch);
    commitCallback.drainTo(callbacks);

    RowBatchMerger merger = localMerger.remove();
    merger.setBatch(batch, callbacks);

    ExecutorService mergerPool = cxt.mergerPool;
    Future<LoadTask> task = mergerPool.submit(merger);
    mergeResult.add(task);
    localMerger.add(merger);

    lastMergeTime = new Date().getTime();
  }

  /**
   * Start {@code ChangeLoader} to work
   *
   * @throws BiremeException last call to {@code ChangeLoader} throw an Exception.
   * @throws InterruptedException interrupted when get the result of last call to
   *         {@code ChangeLoader}.
   */
  public void startLoad() throws BiremeException, InterruptedException {
    Future<LoadTask> head = mergeResult.peek();

    if (head != null && head.isDone()) {
      // get result of last load
      if (loadResult != null && loadResult.isDone()) {
        try {
          loadResult.get();
        } catch (ExecutionException e) {
          throw new BiremeException("Loader failed. ", e.getCause());
        }
      }

      // start a new load
      if (loadResult == null || loadResult.isDone()) {
        loadResult = cxt.loaderPool.submit(loader);
      }
    }
  }

  /**
   * {@code RowBatchMerger} accepts a batch of {@code Rows} and its corresponding
   * {@code CommitCallback}, merge them and create a {@code LoadTask}.
   *
   * @author yuze
   *
   */
  class RowBatchMerger implements Callable<LoadTask> {
    protected ArrayList<Row> rows;
    protected ArrayList<CommitCallback> callbacks;

    /**
     * Set the rows for merge.
     *
     * @param rows the {@code Row}s for merge
     * @param callbacks the corresponding {@code CommitCallback}
     */
    public void setBatch(ArrayList<Row> rows, ArrayList<CommitCallback> callbacks) {
      this.rows = rows;
      this.callbacks = callbacks;
    }

    @Override
    public LoadTask call() {
      LoadTask task = new LoadTask();

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
      }

      rows.clear();
      task.callbacks = callbacks;

      return task;
    }
  }
}
