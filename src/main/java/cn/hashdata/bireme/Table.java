/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * {@code Table} stores table's metadata acquired from database. Metadata includes:
 * <ul>
 * <li><B> Column Name </B> String</li>
 * <li><B> Column Type </B> Integer</li>
 * <li><B> Column Precision </B> Integer</li>
 * <li><B> Column Scale </B> Integer</li>
 * <li><B> Primary Key Index </B> Integer</li>
 * <li><B> Primary Key Name </B> String</li>
 * </ul>
 *
 * @author yuze
 */
public class Table {
  public int ncolumns;
  public ArrayList<String> columnName;
  public HashMap<String, Integer> columnType;
  public HashMap<String, Integer> columnPrecision;
  public HashMap<String, Integer> columnScale;
  public ArrayList<String> keyNames;

  /**
   * Get metadata of a specific table using a given connection and construct a new {@code Table}.
   *
   * @param tableMap  The schema including table
   * @param tableName Table name
   * @param conn      Connection to the database
   * @throws BiremeException - Wrap and throw Exception which cannot be handled.
   */
  public Table(String tableName, Map<String, List<String>> tableMap, Connection conn)
      throws BiremeException {
    this.ncolumns = 0;
    this.columnName = new ArrayList<String>();
    this.keyNames = new ArrayList<String>();
    this.columnType = new HashMap<String, Integer>();
    this.columnPrecision = new HashMap<String, Integer>();
    this.columnScale = new HashMap<String, Integer>();

    Statement statement = null;
    ResultSet rs = null;
    ResultSetMetaData rsMetaData = null;

    try {
      List<String> mapList = tableMap.get(tableName);
      for (int i = 0; i < mapList.size(); i++) {
        this.keyNames.add(mapList.get(i));
      }

      statement = conn.createStatement();

      String queryTableInfo = "select * from public." + tableName + " where 1=2";
      rs = statement.executeQuery(queryTableInfo);
      rsMetaData = rs.getMetaData();
      this.ncolumns = rsMetaData.getColumnCount();

      for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
        String name = rsMetaData.getColumnName(i + 1);
        this.columnName.add(name);
        this.columnType.put(name, rsMetaData.getColumnType(i + 1));
        this.columnPrecision.put(name, rsMetaData.getPrecision(i + 1));
        this.columnScale.put(name, rsMetaData.getScale(i + 1));
      }
    } catch (SQLException e) {
      try {
        conn.close();
      } catch (SQLException ignore) {
      }
      String message = "Could not get metadata for public. " + tableName + ".\n";
      throw new BiremeException(message, e);
    }
  }
}
