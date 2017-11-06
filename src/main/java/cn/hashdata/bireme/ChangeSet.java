/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.util.Date;

/**
 * {@code ChangeSet} is a container which hold data polled from data sources.
 *
 * @author yuze
 *
 */
public class ChangeSet {
  public Date createdAt;
  public Object changes;
  public CommitCallback callback;

  /**
   * Destroy the {@code ChangeSet}
   */
  public void destory() {
    createdAt = null;
    changes = null;
    callback = null;
  }
}
