package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import cn.hashdata.bireme.pipeline.PipeLine;

/**
 * A {@code Dispatcher} is binded with a {@code PipeLine}. It get the transform result and insert
 * into cache.
 *
 * @author yuze
 *
 */
public class Dispatcher {
  public Context cxt;
  public PipeLine pipeLine;
  public RowSet rowSet;
  public boolean complete;
  public LinkedBlockingQueue<Future<RowSet>> transResult;
  public ConcurrentHashMap<String, RowCache> cache;

  public Dispatcher(Context cxt, PipeLine pipeLine) {
    this.cxt = cxt;
    this.pipeLine = pipeLine;
    this.rowSet = null;
    this.complete = false;
    this.transResult = pipeLine.transResult;
    this.cache = pipeLine.cache;
  }

  /**
   * Get the transform result and dispatch.
   *
   * @throws BiremeException transform failed
   * @throws InterruptedException if the current thread was interrupted while waiting
   */
  public void dispatch() throws BiremeException, InterruptedException {
    if (rowSet != null) {
      complete = insertRowSet();

      if (!complete) {
        return;
      }
    }

    while (!transResult.isEmpty() && !cxt.stop) {
      Future<RowSet> head = transResult.peek();

      if (head.isDone()) {
        transResult.remove();
        try {
          rowSet = head.get();
        } catch (ExecutionException e) {
          throw new BiremeException("Transform failed.\n", e.getCause());
        }

        complete = insertRowSet();

        if (!complete) {
          break;
        }
      } else {
        break;
      }
    }
  }

  private boolean insertRowSet() {
    HashMap<String, ArrayList<Row>> bucket = rowSet.rowBucket;
    boolean complete = true;

    ArrayList<Entry<String, ArrayList<Row>>> entrySet =
        new ArrayList<Entry<String, ArrayList<Row>>>();
    entrySet.addAll(bucket.entrySet());

    for (Entry<String, ArrayList<Row>> entry : entrySet) {
      String fullTableName = entry.getKey();
      ArrayList<Row> rows = entry.getValue();
      RowCache rowCache = cache.get(fullTableName);

      if (rowCache == null) {
        rowCache = new RowCache(cxt, fullTableName, pipeLine);
        cache.put(fullTableName, rowCache);
      }

      complete = rowCache.addRows(rows, rowSet.callback);
      if (!complete) {
        break;
      }

      bucket.remove(fullTableName);
      rows.clear();
    }

    if (complete) {
      rowSet.destory();
      rowSet = null;
    }

    return complete;
  }
}
