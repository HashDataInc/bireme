package cn.hashdata.bireme;

import java.util.Date;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * {@code ChangeSet} is a container which hold data polled from data sources.
 *
 * @author yuze
 *
 */
public class ChangeSet {
  public Provider provider;
  public Date createdAt;
  public Object changes;
  public CommitCallback callback;

  /**
   * A implementation of {@code BasePooledObjectFactory} in order to reuse {@code ChangeSet}.
   *
   */
  public static class ChangeSetFactory extends BasePooledObjectFactory<ChangeSet> {
    @Override
    public ChangeSet create() {
      return new ChangeSet();
    }

    @Override
    public PooledObject<ChangeSet> wrap(ChangeSet changeSet) {
      return new DefaultPooledObject<ChangeSet>(changeSet);
    }

    @Override
    public void passivateObject(PooledObject<ChangeSet> pooledObject) {
      ChangeSet changeSet = pooledObject.getObject();
      changeSet.provider = null;
      changeSet.createdAt = null;
      changeSet.changes = null;
      changeSet.callback = null;
    }
  }
}
