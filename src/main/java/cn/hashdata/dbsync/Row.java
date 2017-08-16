package cn.hashdata.dbsync;

import java.util.ArrayList;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * {@code Row} is a dbsync inner format to represent operations to a table. It is transformed from
 * the data polled from any data source. {@code Row} is supposed to contain information about
 * tables, the operation type, operation result, and position in the data source.
 *
 * @author yuze
 *
 */
public class Row {
  public enum RowType { INSERT, UPDATE, DELETE }

  public RowType type;
  public String originTable;
  public String mappedTable;
  public String keys;
  public String oldKeys;
  public String tuple;
  public Position position;

  /**
   * A implementation of {@code BasePooledObjectFactory} in order to reuse {@code Row}.
   */
  public static class RowFactory extends BasePooledObjectFactory<Row> {
    @Override
    public Row create() throws Exception {
      return new Row();
    }

    @Override
    public PooledObject<Row> wrap(Row row) {
      return new DefaultPooledObject<Row>(row);
    }

    @Override
    public void passivateObject(PooledObject<Row> pooledObject) {
      Row row = pooledObject.getObject();
      row.type = null;
      row.originTable = null;
      row.mappedTable = null;
      row.keys = null;
      row.oldKeys = null;
      row.tuple = null;
      row.position = null;
    }
  }

  /**
   * A implementation of {@code BasePooledObjectFactory} in order to reuse {@code ArrayList<Row>}.
   *
   */
  public static class RowArrayFactory extends BasePooledObjectFactory<ArrayList<Row>> {
    @Override
    public ArrayList<Row> create() throws Exception {
      return new ArrayList<Row>();
    }

    @Override
    public PooledObject<ArrayList<Row>> wrap(ArrayList<Row> rowArray) {
      return new DefaultPooledObject<ArrayList<Row>>(rowArray);
    }

    @Override
    public void passivateObject(PooledObject<ArrayList<Row>> pooledObject) {
      ArrayList<Row> rowArray = pooledObject.getObject();
      rowArray.clear();
    }
  }
}
