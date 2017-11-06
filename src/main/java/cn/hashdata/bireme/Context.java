/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.daemon.DaemonController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.MetricRegistry;

import cn.hashdata.bireme.pipeline.PipeLine;

/**
 * bireme context.
 *
 * @author yuze
 *
 */
public class Context {
  static final protected Long TIMEOUT_MS = 1000L;

  public volatile boolean stop = false;

  public Config conf;
  public MetricRegistry register = new MetricRegistry();

  public HashMap<String, String> tableMap;
  public HashMap<String, Table> tablesInfo;

  public LinkedBlockingQueue<Connection> loaderConnections;
  public HashMap<Connection, HashSet<String>> temporaryTables;

  public ArrayList<PipeLine> pipeLines;

  public ExecutorService schedule;
  public Future<Long> scheduleResult;
  public ExecutorService pipeLinePool;
  public ExecutorService transformerPool;
  public ExecutorService mergerPool;
  public ExecutorService loaderPool;

  public WatchDog watchDog;

  public StateServer server;

  public Logger logger = LogManager.getLogger("Bireme.Context");

  /**
   * Create a new bireme context for test.
   *
   * @param conf bireme configuration
   * @throws BiremeException Unknown Host
   */
  public Context(Config conf) throws BiremeException {
    this.conf = conf;

    this.tableMap = conf.tableMap;
    this.tablesInfo = new HashMap<String, Table>();

    this.loaderConnections = new LinkedBlockingQueue<Connection>(conf.loader_conn_size);
    this.temporaryTables = new HashMap<Connection, HashSet<String>>();

    this.pipeLines = new ArrayList<PipeLine>();
    this.schedule = Executors.newSingleThreadExecutor(new BiremeThreadFactory("Scheduler"));

    this.server = new StateServer(this, conf.state_server_addr, conf.state_server_port);

    createThreadPool();
  }

  private void createThreadPool() {
    pipeLinePool =
        Executors.newFixedThreadPool(conf.pipeline_pool_size, new BiremeThreadFactory("PipeLine"));
    transformerPool = Executors.newFixedThreadPool(
        conf.transform_pool_size, new BiremeThreadFactory("Transformer"));
    mergerPool =
        Executors.newFixedThreadPool(conf.merge_pool_size, new BiremeThreadFactory("Merger"));
    loaderPool =
        Executors.newFixedThreadPool(conf.loader_conn_size, new BiremeThreadFactory("Loader"));
  }

  class WatchDog extends Thread {
    private DaemonController controller;
    private Context cxt;

    public WatchDog(DaemonController controller, Context cxt) {
      this.controller = controller;
      this.cxt = cxt;
      this.setDaemon(true);
      this.setName("WatchDog");
    }

    @Override
    public void run() {
      try {
        cxt.waitForStop();
      } catch (InterruptedException e) {
        controller.fail("Service stopped by user");

      } catch (BiremeException e) {
        logger.fatal("Bireme stop abnormally since: {}", e.getMessage());
        logger.fatal("Stack Trace: ", e);
        controller.fail(e);
      }
    }
  }

  public void startWatchDog(DaemonController controller) {
    watchDog = new WatchDog(controller, this);
    watchDog.start();
  }

  /**
   * Start the {@code Scheduler} to schedule the {@code PipeLines} to work.
   */
  public void startScheduler() {
    scheduleResult = schedule.submit(new Scheduler(this));
    server.start();
  }

  /**
   * Wait for bireme to stop and shout down all thread pool.
   *
   * @throws InterruptedException if interrupted while waiting
   * @throws BiremeException scheduler throw out Exception
   */
  public void waitForStop() throws BiremeException, InterruptedException {
    try {
      while (!scheduleResult.isDone()) {
        Thread.sleep(1);
      }
      scheduleResult.get();
    } catch (ExecutionException e) {
      throw new BiremeException("Scheduler abnormally stopped.", e.getCause());

    } finally {
      server.stop();
      schedule.shutdownNow();
      pipeLinePool.shutdownNow();
      transformerPool.shutdownNow();
      mergerPool.shutdownNow();
      loaderPool.shutdownNow();
    }
  }

  /**
   * Wait for all thread pool to terminate.
   */
  public void waitForExit() {
    try {
      schedule.awaitTermination(1, TimeUnit.MINUTES);
      pipeLinePool.awaitTermination(1, TimeUnit.MINUTES);
      transformerPool.awaitTermination(1, TimeUnit.MINUTES);
      mergerPool.awaitTermination(1, TimeUnit.MINUTES);
      loaderPool.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException ignore) {
    }
  }
}

/**
 * For create new Thread.
 *
 * @author yuze
 *
 */
class BiremeThreadFactory implements ThreadFactory {
  private final ThreadGroup group;
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String namePrefix;

  public BiremeThreadFactory(String poolName) {
    SecurityManager s = System.getSecurityManager();
    group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    namePrefix = "pool-" + poolName + "-thread-";
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
    if (t.isDaemon())
      t.setDaemon(false);
    if (t.getPriority() != Thread.NORM_PRIORITY)
      t.setPriority(Thread.NORM_PRIORITY);
    return t;
  }
}
