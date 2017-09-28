/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * {@code LoadTask} is the result of merge operation. A {@code LoadTask} contains a set of data to
 * delete and another set of data to insert. Besides, it also contains positions to be update.
 *
 * @author yuze
 *
 */
public class LoadTask {
  public String tableName;
  public ArrayList<CommitCallback> callbacks;
  public LoadStatus loadStatus;
  public HashSet<String> delete;
  public HashMap<String, String> insert;

  /**
   * Create a new {@code LoadTask}.
   *
   * @param tableName the table this task will be loaded to.
   */
  public LoadTask(String tableName) {
    this.tableName = tableName;
    this.callbacks = new ArrayList<CommitCallback>();
    this.loadStatus = new LoadStatus();
    this.delete = new HashSet<String>();
    this.insert = new HashMap<String, String>();
  }

  public void reset() {
    tableName = null;
    callbacks.clear();
    delete.clear();
    insert.clear();
  }
}
