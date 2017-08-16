package cn.hashdata.dbsync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@code BooKeeper} poll status information offered by change loader. Then it update context and
 * bookkeeping database regularly.
 *
 * @author yuze
 *
 */
public class BookKeeper implements Callable<Long> {
  protected static final Long TIMEOUT_MS = 1000L;

  private Logger logger = LogManager.getLogger("Dbsync." + BookKeeper.class);

  Context cxt;
  String bookKeepingTable;
  ConcurrentHashMap<String, Pair<Position, String>> bookkeeping;
  LinkedBlockingQueue<Triple<String, Position, String>> positionUpdateQueue;

  Connection connUpdateNormal;
  Connection connUpdateError;
  PreparedStatement psError;
  PreparedStatement psNormal;
  Long lastTimeUpdateDB;
  int bookKeepInterval;

  /**
   * Create a new {@code BookKeeper}.
   *
   * @param cxt dbsync context
   */
  public BookKeeper(Context cxt) {
    this.cxt = cxt;
    this.bookkeeping = cxt.bookkeeping;
    this.positionUpdateQueue = cxt.positionUpdateQueue;
    this.bookKeepingTable = cxt.conf.bookkeeping_table;
    this.lastTimeUpdateDB = new Date().getTime();
    this.bookKeepInterval = cxt.conf.bookkeeping_interval;
  }

  /**
   * Call the {@code BookKeeper} to work.
   */
  @Override
  public Long call() throws DbsyncException, InterruptedException {
    Thread.currentThread().setName("Bookkeeper");

    logger.info("BookKeeper Start.");

    try {
      String updateNormal =
          "UPDATE " + bookKeepingTable + " SET POSITION=?, TYPE=?, STATE=? WHERE ORIGIN_TABLE = ?;";
      String updateError = "UPDATE " + bookKeepingTable + " SET STATE=? WHERE ORIGIN_TABLE = ?;";
      connUpdateNormal = Dbsync.jdbcConn(cxt.conf.bookkeeping);
      connUpdateError = Dbsync.jdbcConn(cxt.conf.bookkeeping);
      connUpdateNormal.setAutoCommit(false);
      connUpdateError.setAutoCommit(false);
      psNormal = connUpdateNormal.prepareStatement(updateNormal);
      psError = connUpdateError.prepareStatement(updateError);

    } catch (SQLException e) {
      throw new DbsyncException(e);
    }

    try {
      while (!cxt.stop) {
        HashMap<String, Pair<Position, String>> buffer =
            new HashMap<String, Pair<Position, String>>();
        ArrayList<Triple<String, Position, String>> batch =
            new ArrayList<Triple<String, Position, String>>();

        positionUpdateQueue.drainTo(batch);

        for (Triple<String, Position, String> triple : batch) {
          if (triple.getRight().equals("Error")) {
            String tableName = triple.getLeft();
            triple = Triple.of(tableName, null, "Error");
          }

          buffer.put(triple.getLeft(), Pair.of(triple.getMiddle(), triple.getRight()));
        }

        if (!buffer.isEmpty()) {
          bookkeeping.putAll(buffer);
        }

        buffer.clear();

        if (new Date().getTime() - lastTimeUpdateDB > bookKeepInterval) {
          updateDatabase();
          lastTimeUpdateDB = new Date().getTime();
        } else {
          Thread.sleep(1);
        }
      }
    } catch (DbsyncException e) {
      logger.fatal("BookKeeper exit on error.");
      throw e;

    } finally {
      try {
        connUpdateNormal.close();
      } catch (SQLException ignore) {
      }
    }

    logger.info("BookKeeper exit.");
    return 0L;
  }

  private void updateDatabase() throws DbsyncException {
    String state;
    int countNormal = 0;
    int countError = 0;

    try {
      for (Entry<String, Pair<Position, String>> updateItem : bookkeeping.entrySet()) {
        state = updateItem.getValue().getRight();

        if (state.equals("Normal")) {
          Position position = updateItem.getValue().getLeft();
          // POSITION, TYPE, STATE, ORIGIN_TABLE
          psNormal.setString(1, position.toStirng());
          psNormal.setString(2, position.getType());
          psNormal.setString(3, updateItem.getValue().getRight());
          psNormal.setString(4, updateItem.getKey());
          psNormal.addBatch();
          countNormal++;
        } else {
          psError.setString(1, updateItem.getValue().getRight());
          psError.setString(2, updateItem.getKey());
          psError.addBatch();
          countError++;
        }
      }

      if (countNormal != 0) {
        psNormal.executeBatch();
        connUpdateNormal.commit();
      }

      if (countError != 0) {
        psError.executeBatch();
        connUpdateError.commit();
      }

      psNormal.clearBatch();
      psError.clearBatch();
    } catch (SQLException e) {
      throw new DbsyncException(e.getNextException());
    }
  }
}
