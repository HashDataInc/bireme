/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

/**
 * {@code CommitCallback} is called after a loadtask successfully completed.
 *
 * @author yuze
 *
 */
public interface CommitCallback {
  /**
   * Get the type of this {@code Position}. Usually, each type of data source has a specific method
   * to represent the position.
   *
   * @return the type of this {@code Position}.
   */
  public String getType();

  /**
   * Serialize this {@code Position} in order to store it in the database.
   *
   * @return the serialized {@code Position}.
   */
  public String toStirng();

  /**
   * Restore this {@code Position} according the serialized {@code Position}.
   *
   * @param str the serialized {@code Position}.
   */
  public void fromString(String str);

  /**
   * Set the number of corresponding tables
   *
   * @param tables number of tables
   */
  public void setNumOfTables(int tables);

  /**
   * Commit a successful load task for a table.
   */
  public void done();

  /**
   * Whether this callback is ready to commit
   *
   * @return ready or not
   * @throws BiremeException if this callback has committed
   */
  public boolean ready() throws BiremeException;

  /**
   * Commit this callback.
   */
  public void commit();
}
