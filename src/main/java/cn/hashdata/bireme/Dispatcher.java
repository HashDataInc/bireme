package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import cn.hashdata.bireme.provider.PipeLine;

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

  public Long start() throws BiremeException {
    if (rowSet != null) {
      complete = insertRowSet();

      if (!complete) {
        return 0L;
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
        } catch (InterruptedException e) {
          throw new BiremeException("Get Future<RowSet> failed, be interrupted", e);
        }

        complete = insertRowSet();

        if (!complete) {
          break;
        }
      } else {
        break;
      }
    }

    return 0L;
  }

  private boolean insertRowSet() {
    HashMap<String, ArrayList<Row>> bucket = rowSet.rowBucket;
    boolean complete = true;

    for (Entry<String, ArrayList<Row>> entry : bucket.entrySet()) {
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
      cxt.idleRowArrays.returnObject(rows);
    }

    if (complete) {
      cxt.idleRowSets.returnObject(rowSet);
      rowSet = null;
    }

    return complete;
  }
}
