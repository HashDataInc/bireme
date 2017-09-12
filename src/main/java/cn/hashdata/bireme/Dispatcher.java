package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.hashdata.bireme.Provider.Transformer;

/**
 * {@code Dispatcher} constantly poll {@code ChangeSet} and offer the {@code ChangeSet} to right
 * {@code Transformer}. After transformation complete, {@code Dispatcher} insert the result to
 * suitable {@code RowCache}.
 *
 * @author yuze
 *
 */
public class Dispatcher implements Callable<Long> {
  static final protected Long TIMEOUT_MS = 1000L;

  private Logger logger = LogManager.getLogger("Bireme." + Dispatcher.class);

  protected Context cxt;
  protected ChangeSet changeSet;
  protected LinkedBlockingQueue<ChangeSet> changeSetIn;
  protected ExecutorService transformerThreadPool;
  protected CompletionService<RowSet> cs;
  protected LinkedBlockingQueue<Future<RowSet>> results;
  protected LinkedBlockingQueue<Transformer> transformers;

  /**
   * Create a new {@code Dispatcher}.
   *
   * @param cxt bireme context.
   */
  public Dispatcher(Context cxt) {
    this.cxt = cxt;
    changeSetIn = cxt.changeSetQueue;
    transformerThreadPool = Executors.newFixedThreadPool(cxt.conf.transform_pool_size);
    cs = new ExecutorCompletionService<RowSet>(transformerThreadPool);
    results = new LinkedBlockingQueue<Future<RowSet>>(cxt.conf.trans_result_queue_size);
    transformers = new LinkedBlockingQueue<Transformer>(cxt.conf.trans_result_queue_size * 2);
  }

  /**
   * Call the {@code Dispatcher} to work.
   */
  public Long call() throws BiremeException, InterruptedException {
    Thread.currentThread().setName("Dispatcher");

    logger.info("Dispatcher Start.");

    try {
      while (!cxt.stop) {
        do {
          changeSet = changeSetIn.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
          checkTansformResults();
        } while (changeSet == null && !cxt.stop);

        if (cxt.stop) {
          break;
        }

        dispatch(changeSet);
      }
    } catch (BiremeException e) {
      logger.fatal("Dispatcher exit on error: " + e.getMessage());
      throw e;
    } finally {
      transformerThreadPool.shutdown();
    }

    logger.info("Dispatcher exit.");
    return 0L;
  }

  private void dispatch(ChangeSet changeSet) throws BiremeException, InterruptedException {
    Transformer transformer = changeSet.provider.borrowTransformer(changeSet);
    transformers.put(transformer);
    Future<RowSet> result = cs.submit(transformer);
    boolean success;

    do {
      success = results.offer(result, TIMEOUT_MS, TimeUnit.MILLISECONDS);
      checkTansformResults();
    } while (!success && !cxt.stop);
  }

  private void checkTansformResults() throws BiremeException, InterruptedException {
    RowSet rowSet = null;
    while (!results.isEmpty() && !cxt.stop) {
      Future<RowSet> head = results.peek();

      if (head.isDone()) {
        results.remove();

        Transformer trans = transformers.take();
        Provider provider = trans.getProvider();
        provider.returnTransformer(trans);

        try {
          rowSet = head.get();
        } catch (ExecutionException e) {
          throw new BiremeException(e.getCause());
        }

        insertRowSet(rowSet);
      } else {
        break;
      }
    }
  }

  private void insertRowSet(RowSet rowSet) throws InterruptedException, BiremeException {
    HashMap<String, ArrayList<Row>> bucket = rowSet.rowBucket;
    ConcurrentHashMap<String, RowCache> tableCache = cxt.tableRowCache;

    try {
      for (Entry<String, ArrayList<Row>> entry : bucket.entrySet()) {
        String fullTableName = entry.getKey();
        ArrayList<Row> rows = entry.getValue();
        RowCache cache = tableCache.get(fullTableName);
        cache.addRows(rows, rowSet.callback);
        cxt.idleRowArrays.returnObject(rows);
      }
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new BiremeException(e);
    }

    cxt.idleRowSets.returnObject(rowSet);
  }
}
