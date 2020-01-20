/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;


/**
 * {@code Row} is a bireme inner format to represent operations to a table. It is transformed from
 * the data polled from any data source. {@code Row} is supposed to contain information about
 * tables, the operation type, operation result, and position in the data source.
 *
 * @author yuze
 *
 */
public class Row {
  public enum RowType { INSERT, UPDATE, DELETE }
  public enum DDLType {CREATE_DATABASE, DROP_DATABASE, ALTER_DATABASE, CREATE_TABLE, DROP_TABLE, ALTER_TABLE}

  public Long produceTime;
  public RowType type;
  public DDLType ddlType;
  public String originTable;
  public String mappedTable;
  public String keys;
  public String oldKeys;
  public String tuple;
  public String ddlSQL;
  public String database;
  public String table;
  public TableSchema def;
  public boolean isDDL = false;

  @Override
  public String toString() {
    return "Row{" +
            "produceTime=" + produceTime +
            ", type=" + type +
            ", ddlType=" + ddlType +
            ", originTable='" + originTable + '\'' +
            ", mappedTable='" + mappedTable + '\'' +
            ", keys='" + keys + '\'' +
            ", oldKeys='" + oldKeys + '\'' +
            ", tuple='" + tuple + '\'' +
            ", ddlSQL='" + ddlSQL + '\'' +
            ", database='" + database + '\'' +
            ", table='" + table + '\'' +
            ", def=" + def +
            ", isDDL=" + isDDL +
            '}';
  }
}
