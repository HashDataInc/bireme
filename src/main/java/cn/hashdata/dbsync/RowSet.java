package cn.hashdata.dbsync;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * {@code RowSet} contains a set of transformed {@code Rows}.
 *
 * @author yuze
 */
public class RowSet {
  public Date createdAt;
  public HashMap<String, ArrayList<Row>> rowBucket;

  public RowSet() {
    rowBucket = new HashMap<String, ArrayList<Row>>();
  }

  /**
   * A implementation of {@code BasePooledObjectFactory} in order to reuse {@code RowSet}.
   */
  public static class RowSetFactory extends BasePooledObjectFactory<RowSet> {
    @Override
    public RowSet create() throws Exception {
      return new RowSet();
    }

    @Override
    public PooledObject<RowSet> wrap(RowSet rowSet) {
      return new DefaultPooledObject<RowSet>(rowSet);
    }

    @Override
    public void passivateObject(PooledObject<RowSet> pooledObject) {
      RowSet rowSet = pooledObject.getObject();
      rowSet.createdAt = null;
      rowSet.rowBucket.clear();
    }
  }
}
