package cn.hashdata.bireme;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 *
 *
 * @author zuanqi
 */
public class GetPrimaryKeys {
  private static Logger logger = LogManager.getLogger("Bireme." + GetPrimaryKeys.class);

  public static Map<String, List<String>> getPrimaryKeys(
      HashMap<String, String> tableMap, Connection conn) throws Exception {
    Statement statement = null;
    ResultSet resultSet = null;
    ResultSet tableRs = null;
    Map<String, List<String>> table_map = new HashMap<>();
    List<String> checkTableMap = new ArrayList<>();
    String[] strArray;
    StringBuilder sb = new StringBuilder();
    sb.append("(");

    for (String fullname : tableMap.values()) {
      strArray = fullname.split("\\.");
      sb.append("'").append(strArray[1]).append("',");
    }

    String tableList = sb.toString().substring(0, sb.toString().length() - 1) + ")";
    String tableSql = "select tablename from pg_tables where schemaname='public' and tablename in "
        + tableList + "";
    String prSql = "SELECT NULL AS TABLE_CAT, "
        + "n.nspname  AS TABLE_SCHEM, "
        + "ct.relname AS TABLE_NAME, "
        + "a.attname  AS COLUMN_NAME, "
        + "(i.keys).n AS KEY_SEQ, "
        + "ci.relname AS PK_NAME "
        + "FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) "
        + "JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
        + "JOIN ( SELECT i.indexrelid, i.indrelid, i.indisprimary, information_schema._pg_expandarray(i.indkey) AS KEYS FROM pg_catalog.pg_index i) i ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) "
        + "JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE TRUE AND n.nspname = 'public' AND ct.relname in "
        + tableList + " AND i.indisprimary ORDER BY TABLE_NAME, pk_name, key_seq";
    try {
      statement = conn.createStatement();
      tableRs = statement.executeQuery(tableSql);

      while (tableRs.next()) {
        checkTableMap.add(tableRs.getString("tablename"));
      }

      resultSet = statement.executeQuery(prSql);
      while (resultSet.next()) {
        String tableName = resultSet.getString("TABLE_NAME");
        if (table_map.containsKey(tableName)) {
          List<String> strings = table_map.get(tableName);
          strings.add(resultSet.getString("COLUMN_NAME"));
        } else {
          List<String> multiPKList = new ArrayList<>();
          multiPKList.add(resultSet.getString("COLUMN_NAME"));
          table_map.put(tableName, multiPKList);
        }
      }

      if (table_map.size() != tableMap.size()) {
        String message = "Greenplum table and MySQL table size are inconsistent!";
        throw new BiremeException(message);
      } else {
        logger.info("MySQL、Greenplum table check completed, the state is okay！");
      }

      if (table_map.size() != tableMap.size()) {
        String message = "some tables do not have primary keys！";
        throw new BiremeException(message);
      } else {
        logger.info("Greenplum table primary key check is completed, the state is okay！");
      }

    } catch (SQLException e) {
      try {
        statement.close();
        conn.close();
      } catch (SQLException ignore) {
      }
      String message = "Could not get PrimaryKeys";
      throw new BiremeException(message, e);
    }

    return table_map;
  }
}
