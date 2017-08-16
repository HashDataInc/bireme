package cn.hashdata.dbsync;

/**
 * {@code Position} is used to mark the position of one message in its data source.
 *
 * @author yuze
 *
 */
public interface Position {
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
   * Compare two {@code Position}.
   *
   * @param other another {@code Position} to compare with
   * @return {@code true} if this {@code Position} is less or equal than the other, else
   *         {@code false}
   * @throws DbsyncException - Wrap and throw Exception which cannot be handled
   */
  public boolean lessEqual(Position other) throws DbsyncException;
}
