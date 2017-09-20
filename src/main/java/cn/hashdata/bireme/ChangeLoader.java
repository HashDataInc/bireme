/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * {@code ChangeLoader} poll tasks and load the tasks to database. Each {@code ChangeLoader}
 * corresponds to a specific table. All {@code ChangeLoaders} share connections to the database.
 *
 * @author yuze
 *
 */
public class ChangeLoader implements Callable<Long> {
  protected static final Long TIMEOUT_MS = 1000L;
  protected static final Long DELETE_TIMEOUT_NS = 10000000000L;

  private Logger logger = LogManager.getLogger("Bireme." + ChangeLoader.class);

  protected boolean optimisticMode = true;
  protected Context cxt;
  protected Config conf;
  protected Connection conn;
  protected LinkedBlockingQueue<Future<LoadTask>> taskIn;
  protected Table table;
  protected LoadTask currentTask;
  ExecutorService threadPool;
  private String mappedTable;
  protected LinkedBlockingQueue<Triple<String, CommitCallback, String>> positionUpdateQueue;

  private Timer copyForDeleteTimer;
  private Timer deleteTimer;
  private Timer copyForInsertTimer;
  private Timer.Context timerCTX;

  /**
   * Create a new {@code ChangeLoader}.
   *
   * @param cxt bireme context
   * @param mappedTable the corresponding table
   */
  public ChangeLoader(Context cxt, String mappedTable) {
    this.cxt = cxt;
    this.conf = cxt.conf;
    this.conn = null;
    this.mappedTable = mappedTable;
    this.table = cxt.tablesInfo.get(mappedTable);
    taskIn = new LinkedBlockingQueue<Future<LoadTask>>(conf.loader_task_queue_size);
    threadPool = Executors.newFixedThreadPool(1);

    cxt.metrics.register(
        MetricRegistry.name(Context.class, "TaskQueue for " + mappedTable), new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return taskIn.size();
          }
        });

    copyForDeleteTimer =
        cxt.metrics.timer(MetricRegistry.name(ChangeLoader.class, mappedTable, "CopyForDelete"));
    deleteTimer = cxt.metrics.timer(MetricRegistry.name(ChangeLoader.class, mappedTable, "Delete"));
    copyForInsertTimer =
        cxt.metrics.timer(MetricRegistry.name(ChangeLoader.class, mappedTable, "CopyForInsert"));
  }

  /**
   * Call the {@code ChangeLoader} to work.
   */
  public Long call() {
    Thread.currentThread().setName("ChangeLoader for " + mappedTable);

    logger.info("Loader Start, corresponding table {}.", mappedTable);

    try {
      while (!cxt.stop) {
        // get task
        currentTask = pollTask();

        if (cxt.stop) {
          break;
        }

        // get connection
        getConnection();

        if (cxt.stop) {
          break;
        }

        try {
          executeTask();
        } catch (Exception e) {
          try {
            conn.rollback();
          } catch (Exception ignore) {
          }
          throw e;
        } finally {
          releaseConnection();
          currentTask.reset();
          currentTask = null;
        }
      }
    } catch (InterruptedException ignore) {
    } catch (Exception e) {
      logger.error(
          "Loader exit on error, corresponding table {}. Message {}", mappedTable, e.getMessage());
      return 0L;
    } finally {
      threadPool.shutdown();
    }

    logger.info("Loader exit, corresponding table {}.", mappedTable);
    return 0L;
  }

  /**
   * Check whether {@code Rows} have been merged to a task. If done, poll the task and return.
   *
   * @return a task need be load to database
   * @throws InterruptedException - if interrupted while waiting
   * @throws BiremeException - wrap and throw Exception which cannot be handled
   */
  protected LoadTask pollTask() throws InterruptedException, BiremeException {
    LoadTask task = null;

    while (!cxt.stop) {
      Future<LoadTask> head = taskIn.peek();

      if (head != null && head.isDone()) {
        taskIn.remove();

        try {
          task = head.get();
        } catch (ExecutionException e) {
          throw new BiremeException(e.getCause());
        }

        if (task != null) {
          break;
        }
      }

      Thread.sleep(1);
    }

    return task;
  }

  /**
   * Get connection to the destination database from connection pool.
   *
   * @throws InterruptedException - if interrupted while waiting
   * @throws BiremeException - wrap and throw Exception which cannot be handled
   */
  protected void getConnection() throws InterruptedException, BiremeException {
    do {
      conn = cxt.loaderConnections.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } while (conn == null && !cxt.stop);

    HashSet<String> temporaryTables = cxt.temporaryTables.get(conn);

    if (!temporaryTables.contains(mappedTable)) {
      createTemporaryTable();
      temporaryTables.add(mappedTable);
    }
  }

  /**
   * Return the connection to connection pool.
   *
   * @throws InterruptedException - if interrupted while waiting
   */
  protected void releaseConnection() throws InterruptedException {
    boolean success;

    do {
      success = cxt.loaderConnections.offer(conn, TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } while (!success && !cxt.stop);

    conn = null;
  }

  /**
   * Load the task to destination database. First load the delete set and then load the insert set.
   *
   * @throws InterruptedException - if interrupted while waiting
   * @throws BiremeException - Wrap and throw Exception which cannot be handled
   */
  protected void executeTask() throws InterruptedException, BiremeException {
    if (!currentTask.delete.isEmpty() || (!optimisticMode && !currentTask.insert.isEmpty())) {
      int size = currentTask.delete.size();

      if (!optimisticMode) {
        currentTask.delete.addAll(currentTask.insert.keySet());
      }

      if (executeDelete(currentTask.delete) <= size && optimisticMode == false) {
        optimisticMode = true;

        logger.info("Chang to optimistic mode.");
      }

      // After delete, commit
      try {
        if (cxt.stop) {
          conn.rollback();
          return;
        } else {
          conn.commit();
        }
      } catch (SQLException e) {
        throw new BiremeException(e);
      }
    }

    if (!currentTask.insert.isEmpty()) {
      HashSet<String> insertSet = new HashSet<String>();
      insertSet.addAll(currentTask.insert.values());
      executeInsert(insertSet);

      try {
        if (cxt.stop) {
          conn.rollback();
          return;
        } else {
          conn.commit();
        }
      } catch (SQLException e) {
        throw new BiremeException(e);
      }
    }

    for (CommitCallback callback : currentTask.callbacks) {
      callback.done();
    }
  }

  private Long executeDelete(Set<String> delete) throws BiremeException, InterruptedException {
    long deleteCounts;
    ArrayList<String> keyNames = table.keyNames;
    String temporaryTableName = getTemporaryTableName();

    timerCTX = copyForDeleteTimer.time();
    copyWorker(temporaryTableName, keyNames, delete);
    timerCTX.stop();

    if (cxt.stop) {
      return 0L;
    }

    timerCTX = deleteTimer.time();
    deleteCounts = deleteWorker(mappedTable, temporaryTableName, keyNames);

    if (timerCTX.stop() > DELETE_TIMEOUT_NS) {
      String plan = deletePlan(mappedTable, temporaryTableName, keyNames);

      logger.warn("Delete time exceed 10 seconds, delete plan:\n {}", plan);
    }

    return deleteCounts;
  }

  private void executeInsert(Set<String> insertSet) throws BiremeException, InterruptedException {
    ArrayList<String> columnList = table.columnName;

    timerCTX = copyForInsertTimer.time();
    try {
      copyWorker(mappedTable, columnList, insertSet);
    } catch (BiremeException e) {
      if (e.getCause().getMessage().contains("duplicate key value") && optimisticMode) {
        try {
          conn.rollback();
        } catch (SQLException ignore) {
        }

        optimisticMode = false;

        logger.info("Chang to passimistic mode.");

        executeDelete(currentTask.insert.keySet());
        executeInsert(insertSet);
      } else {
        throw e;
      }
    }

    timerCTX.stop();
  }

  private Long copyWorker(String tableName, ArrayList<String> columnList, Set<String> tuples)
      throws BiremeException, InterruptedException {
    Future<Long> copyResult;
    long copyCount = -1L;
    PipedOutputStream pipeOut = new PipedOutputStream();
    PipedInputStream pipeIn = null;

    try {
      pipeIn = new PipedInputStream(pipeOut);
    } catch (IOException e) {
      throw new BiremeException(e);
    }

    String sql = getCopySql(tableName, columnList);
    copyResult = threadPool.submit(new TupleCopyer(pipeIn, sql, conn));

    tupleWriter(pipeOut, tuples);

    try {
      while (!copyResult.isDone() && !cxt.stop) {
        Thread.sleep(1);
      }

      if (cxt.stop) {
        return 0L;
      }

      copyCount = copyResult.get();
    } catch (ExecutionException e) {
      throw new BiremeException(e.getCause());
    }

    return copyCount;
  }

  private String getCopySql(String tableName, List<String> columnList) {
    StringBuilder sb =
        new StringBuilder()
            .append("COPY ")
            .append(tableName)
            .append(" (")
            .append(StringUtils.join(columnList, ","))
            .append(") FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '\"' ESCAPE E'\\\\';");
    String sql = sb.toString();
    return sql;
  }

  private Long deleteWorker(String table, String tmpTable, ArrayList<String> columnList)
      throws BiremeException {
    StringBuilder sb = new StringBuilder();
    Long count = 0L;

    for (int i = 0; i < columnList.size(); i++) {
      if (i != 0) {
        sb.append(" and ");
      }

      sb.append(table + "." + columnList.get(i) + "=" + tmpTable + "." + columnList.get(i));
    }

    String sql = "DELETE FROM " + table + " WHERE EXISTS (SELECT 1 FROM " + tmpTable + " WHERE "
        + sb.toString() + ");";

    try {
      count = (long) conn.createStatement().executeUpdate(sql);
    } catch (SQLException e) {
      throw new BiremeException(e);
    }

    return count;
  }

  private String deletePlan(String table, String tmpTable, ArrayList<String> columnList)
      throws BiremeException {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < columnList.size(); i++) {
      if (i != 0) {
        sb.append(" and ");
      }

      sb.append(table + "." + columnList.get(i) + "=" + tmpTable + "." + columnList.get(i));
    }

    String sql = "EXPLAIN DELETE FROM " + table + " WHERE EXISTS (SELECT 1 FROM " + tmpTable
        + " WHERE " + sb.toString() + ");";

    try {
      ResultSet rs = conn.createStatement().executeQuery(sql);

      if (!rs.wasNull()) {
        sb.setLength(0);

        while (rs.next()) {
          sb.append(rs.getString(1) + "\n");
        }

        return sb.toString();

      } else {
        return "Can not get plan.";
      }

    } catch (SQLException e) {
      throw new BiremeException(e);
    }
  }

  private class TupleCopyer implements Callable<Long> {
    PipedInputStream pipeIn;
    String sql;
    Connection conn;

    public TupleCopyer(PipedInputStream pipeIn, String sql, Connection conn) {
      this.pipeIn = pipeIn;
      this.sql = sql;
      this.conn = conn;
    }

    @Override
    public Long call() throws SQLException, IOException {
      try {
        CopyManager mgr = new CopyManager((BaseConnection) conn);
        return mgr.copyIn(sql, pipeIn);
      } finally {
        try {
          pipeIn.close();
        } catch (IOException ignore) {
        }
      }
    }
  }

  private void tupleWriter(PipedOutputStream pipeOut, Set<String> tuples) throws BiremeException {
    byte[] data = null;

    try {
      Iterator<String> iterator = tuples.iterator();

      while (iterator.hasNext() && !cxt.stop) {
        data = iterator.next().getBytes("UTF-8");
        pipeOut.write(data);
      }

      pipeOut.flush();
    } catch (IOException e) {
      throw new BiremeException(e);
    } finally {
      try {
        pipeOut.close();
      } catch (IOException ignore) {
      }
    }
  }

  private String getTemporaryTableName() {
    return mappedTable.replace('.', '_');
  }

  private void createTemporaryTable() throws BiremeException {
    String sql = "CREATE TEMP TABLE " + getTemporaryTableName()
        + " ON COMMIT DELETE ROWS AS SELECT * FROM " + mappedTable + " LIMIT 0;";

    try {
      conn.createStatement().executeUpdate(sql);
      conn.commit();
    } catch (SQLException e) {
      throw new BiremeException(e);
    }
  }
}
