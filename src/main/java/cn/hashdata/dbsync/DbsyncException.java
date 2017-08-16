package cn.hashdata.dbsync;

/**
 * {@code DbsyncException} is used to wrap {@code Exception} dbsync cannot handle by itself.
 *
 * @author yuze
 *
 */
public class DbsyncException extends Exception {
  private static final long serialVersionUID = 1L;

  public DbsyncException() {}

  public DbsyncException(String message) {
    super(message);
  }

  public DbsyncException(String message, Throwable cause) {
    super(message, cause);
  }

  public DbsyncException(Throwable cause) {
    super(cause);
  }
}
