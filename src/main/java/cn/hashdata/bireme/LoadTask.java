/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * {@code LoadTask} is the result of merge operation. A {@code LoadTask} contains a set of data to
 * delete and another set of data to insert. Besides, it also contains {code CommitCallback} to be
 * committed.
 *
 * @author yuze
 *
 */
public class LoadTask {
  public ArrayList<CommitCallback> callbacks;
  public HashSet<String> delete;
  public HashMap<String, String> insert;

  /**
   * Create a new {@code LoadTask}.
   */
  public LoadTask() {
    this.callbacks = new ArrayList<CommitCallback>();
    this.delete = new HashSet<String>();
    this.insert = new HashMap<String, String>();
  }

  /**
   * Destroy the {@code LoadTask}.
   */
  public void destory() {
    callbacks.clear();
    delete.clear();
    insert.clear();
  }
}
