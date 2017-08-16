package cn.hashdata.dbsync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.hashdata.dbsync.RowCache.RowBatchMerger;

/**
 * {@code TaskGenerator} constantly check every {@code RowCache} and trigger the
 * {@code RowBatchMerger} to work. It also offer the merge result to corresponding
 * {@code ChangeLoader}.
 *
 * @author yuze
 *
 */
public class TaskGenerator implements Callable<Long> {
  protected static final Long TIMEOUT_MS = 1000L;

  private Logger logger = LogManager.getLogger("Dbsync." + TaskGenerator.class);

  protected Context cxt;
  protected Config conf;
  protected ConcurrentHashMap<String, RowCache> tableCache;
  protected ExecutorService threadPool;
  protected CompletionService<LoadTask> cs;
  protected HashMap<String, ChangeLoader> changeLoaders;

  public TaskGenerator(Context cxt) {
    this.cxt = cxt;
    tableCache = cxt.tableRowCache;
    conf = cxt.conf;
    changeLoaders = cxt.changeLoaders;
    threadPool = Executors.newFixedThreadPool(conf.merge_pool_size);
    cs = new ExecutorCompletionService<LoadTask>(threadPool);
  }

  /**
   * Constantly check the {@code RowCache} and construct {@code RowBatchMerger} to work.
   */
  public Long call() throws DbsyncException, InterruptedException, Exception {
    Thread.currentThread().setName("TaskGenerator");

    logger.info("TaskGenerator Start.");

    try {
      while (!cxt.stop) {
        generateMergeTask();

        if (cxt.stop) {
          break;
        }

        Thread.sleep(1);
      }

    } catch (DbsyncException e) {
      logger.fatal("TaskGenerator exit on error: " + e.getMessage());
      throw e;
    } finally {
      threadPool.shutdown();
    }

    logger.info("TaskGenerator exit.");
    return 0L;
  }

  private void generateMergeTask() throws InterruptedException, Exception {
    Future<LoadTask> task;

    for (Entry<String, RowCache> entry : tableCache.entrySet()) {
      String table = entry.getKey();
      RowCache cache = entry.getValue();
      LinkedBlockingQueue<Future<LoadTask>> taskQueue = changeLoaders.get(table).taskIn;

      if (taskQueue.remainingCapacity() <= 0) {
        continue;
      }

      ArrayList<Row> batch = cache.createBatch(conf.merge_interval, conf.batch_size);

      if (cxt.stop) {
        break;
      }

      if (batch != null) {
        logger.trace("Create batch {}.", batch.hashCode());

        task = cs.submit(new RowBatchMerger(entry.getKey(), batch, cxt));
        boolean success;

        do {
          success = taskQueue.offer(task, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } while (!success && !cxt.stop);
      }
    }
  }
}
