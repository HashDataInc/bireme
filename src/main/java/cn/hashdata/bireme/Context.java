/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.daemon.DaemonController;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import cn.hashdata.bireme.ChangeSet.ChangeSetFactory;
import cn.hashdata.bireme.Row.RowArrayFactory;
import cn.hashdata.bireme.Row.RowFactory;
import cn.hashdata.bireme.RowSet.RowSetFactory;
import cn.hashdata.bireme.provider.Provider;

/**
 * bireme context.
 *
 * @author yuze
 *
 */
public class Context {
  static final protected Long TIMEOUT_MS = 1000L;
  static final protected int AUX_THREAD = 3;

  public volatile boolean stop = false;

  public Config conf;
  public MetricRegistry metrics = new MetricRegistry();

  public HashMap<String, String> tableMap;
  public HashMap<String, Table> tablesInfo;

  public ArrayList<Provider> pipeLines;

  public HashMap<String, ChangeLoader> changeLoaders;
  public LinkedBlockingQueue<Connection> loaderConnections;
  public HashMap<Connection, HashSet<String>> temporaryTables;

  public GenericObjectPool<ChangeSet> idleChangeSets;
  public GenericObjectPool<RowSet> idleRowSets;
  public GenericObjectPool<Row> idleRows;
  public GenericObjectPool<ArrayList<Row>> idleRowArrays;

  public ExecutorService scheduleThread;
  public ExecutorService transformerPool;
  public ExecutorService dispatcherPool;
  public ExecutorService mergerPool;

  public ExecutorService loaderThreadPool;
  public CompletionService<Long> loadercs;

  public int exitLoaders;
  public WatchDog watchDog;

  public StateServer server;

  /**
   * Create a new bireme context.
   *
   * @param conf bireme configuration
   * @throws BiremeException Unknown Host
   */
  public Context(Config conf) throws BiremeException {
    this(conf, false);
  }

  /**
   * Create a new bireme context for test.
   *
   * @param conf bireme configuration
   * @param test unitest or not
   * @throws BiremeException Unknown Host
   */
  public Context(Config conf, Boolean test) throws BiremeException {
    this.conf = conf;

    this.tableMap = conf.tableMap;
    this.tablesInfo = new HashMap<String, Table>();

    this.changeLoaders = new HashMap<String, ChangeLoader>();
    this.loaderConnections = new LinkedBlockingQueue<Connection>(conf.loader_conn_size);
    this.temporaryTables = new HashMap<Connection, HashSet<String>>();
    this.pipeLines = new ArrayList<Provider>();

    this.server = new StateServer(this, conf.state_server_addr, conf.state_server_port);

    exitLoaders = 0;

    createObjectPool();

    if (!test) {
      createThreadPool();
      // registerGauge();
    }
  }

  private void createObjectPool() {
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setBlockWhenExhausted(false);
    config.setMaxTotal(-1);
    config.setMaxIdle(-1);
    config.setMinIdle(0);
    config.setMaxWaitMillis(-1);

    config.setJmxNamePrefix("idleChangeSets");
    idleChangeSets = new GenericObjectPool<ChangeSet>(new ChangeSetFactory(), config);
    config.setJmxNamePrefix("idleRowSets");
    idleRowSets = new GenericObjectPool<RowSet>(new RowSetFactory(), config);
    config.setJmxNamePrefix("idleRows");
    idleRows = new GenericObjectPool<Row>(new RowFactory(), config);
    config.setJmxNamePrefix("idleRowArrays");
    idleRowArrays = new GenericObjectPool<ArrayList<Row>>(new RowArrayFactory(), config);
  }

  private void createThreadPool() {
    // TODO add pool size in config file
    scheduleThread = Executors.newSingleThreadExecutor();
    transformerPool = Executors.newFixedThreadPool(conf.transform_pool_size);
    dispatcherPool = Executors.newFixedThreadPool(conf.transform_pool_size);
    mergerPool = Executors.newFixedThreadPool(conf.merge_pool_size);
    // #loader
    loaderThreadPool = Executors.newFixedThreadPool(conf.loadersCount);
    loadercs = new ExecutorCompletionService<Long>(loaderThreadPool);
  }

  private void registerGauge() {
    metrics.register(MetricRegistry.name(Context.class, "ChangeSetQueue"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        // TODO return changeSetQueue.size();
        return 0;
      }
    });

    /* TODO
    for (Entry<String, RowCache> entry : tableRowCache.entrySet()) {
      String fullTableName = entry.getKey();
      RowCache rowCache = entry.getValue();
      metrics.register(MetricRegistry.name(RowCache.class, "for " + fullTableName),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return rowCache.rows.size();
            }
          });
    }
    */
  }

  static class WatchDog extends Thread {
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
        cxt.waitForComplete(false);
      } catch (InterruptedException e) {
        controller.fail("Service stopped by user");
      } catch (Exception e) {
        controller.fail(e);
      }
    }
  }

  public void startWatchDog(DaemonController controller) {
    watchDog = new WatchDog(controller, this);
    watchDog.start();
  }

  /**
   * Wait for all threads to exit.
   *
   * @param ignoreError whether to ignore error.
   * @throws InterruptedException if interrupted while waiting
   * @throws BiremeException thread exit abnormally
   */
  public void waitForComplete(boolean ignoreError) throws BiremeException, InterruptedException {
    /*
    while (!threadPool.isTerminated() && !loaderThreadPool.isTerminated()) {
      Throwable cause = null;

      if (!threadPool.isTerminated()) {
        Future<Long> result = cs.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if (result != null) {
          try {
            result.get();
          } catch (ExecutionException e) {
            cause = e.getCause();
          }
        }

        if (cause != null && !ignoreError) {
          try {
            throw cause;
          } catch (BiremeException | InterruptedException | RuntimeException e) {
            throw e;
          } catch (Throwable e) {
            throw new BiremeException(e.getMessage(), e);
          }
        }
      }

      if (!loaderThreadPool.isTerminated()) {
        Future<Long> loaderResult = loadercs.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if (loaderResult != null) {
          try {
            loaderResult.get();
          } catch (ExecutionException e) {
            cause = e.getCause();
          } finally {
            exitLoaders++;
          }

          if (exitLoaders == conf.loadersCount) {
            for (Connection conn : loaderConnections) {
              try {
                conn.close();
              } catch (Exception ignore) {
              }
            }

            loaderConnections.clear();

            if (!stop) {
              throw new BiremeException("All loaders failed.");
            }
          }
        }

        if (cause != null && !ignoreError) {
          try {
            throw cause;
          } catch (BiremeException | InterruptedException | RuntimeException e) {
            throw e;
          } catch (Throwable e) {
            throw new BiremeException(e.getMessage(), e);
          }
        }
      }
    } */
  }
}
