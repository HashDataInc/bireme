/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * {@code RowSet} contains a set of transformed {@code Rows}.
 *
 * @author yuze
 */
public class RowSet {
  public Date createdAt;
  public HashMap<String, ArrayList<Row>> rowBucket;
  public CommitCallback callback;

  public RowSet() {
    rowBucket = new HashMap<String, ArrayList<Row>>();
  }

  /**
   * Destroy the {@code RowSet}.
   */
  public void destory() {
    createdAt = null;
    rowBucket.clear();
    rowBucket = null;
    callback = null;
  }
}
