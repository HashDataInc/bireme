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

  public Long produceTime;
  public RowType type;
  public String originTable;
  public String mappedTable;
  public String keys;
  public String oldKeys;
  public String tuple;
}
