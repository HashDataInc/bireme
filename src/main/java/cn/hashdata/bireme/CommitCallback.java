/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

/**
 * {@code CommitCallback} traces a {@link ChangeSet}. After the {@code ChangeSet} is loaded, mark it
 * as committed.
 *
 * @author yuze
 *
 */
public interface CommitCallback {
  /**
   * Set the number of corresponding tables.
   *
   * @param tables number of tables
   */
  public void setNumOfTables(int tables);

  /**
   * Commit a successful load task for a table.
   */
  public void done();

  /**
   * Whether this callback is ready to commit.
   *
   * @return ready or not
   * @throws BiremeException if this callback has committed
   */
  public boolean ready() throws BiremeException;

  /**
   * Commit this callback.
   */
  public void commit();

  /**
   * Set the produce time of the newest record in corresponding set.
   *
   * @param time the produce time
   */
  public void setNewestRecord(Long time);

  /**
   * Destory this {@code CommitCallback} to release the memory.
   */
  public void destory();
}
