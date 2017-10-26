/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

/**
 * {@code BiremeException} is used to wrap Exceptions bireme cannot handle by itself.
 *
 * @author yuze
 *
 */
public class BiremeException extends Exception {
  private static final long serialVersionUID = 1L;

  public BiremeException() {}

  public BiremeException(String message) {
    super(message);
  }

  public BiremeException(String message, Throwable cause) {
    super(message, cause);
  }
}
