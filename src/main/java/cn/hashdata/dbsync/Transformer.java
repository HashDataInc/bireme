package cn.hashdata.dbsync;

import java.util.concurrent.Callable;

/**
 * {@code Transformer} is responsible for transforming origin data to {@code Rows}.
 */
public abstract class Transformer implements Callable<RowSet> {
  protected ChangeSet changeSet;
  protected Provider provider;

  /**
   * Designate the ChangeSet.
   *
   * @param changeSet The set of origin data need to be transformed.
   */
  public void setChangeSet(ChangeSet changeSet) {
    this.changeSet = changeSet;

    if (changeSet != null) {
      this.provider = changeSet.provider;
    } else {
      this.provider = null;
    }
  }

  @Override public abstract RowSet call() throws Exception;

  public Provider getProvider() {
    return this.provider;
  }
}
