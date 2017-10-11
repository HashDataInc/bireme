package cn.hashdata.bireme;

import java.util.HashMap;

/**
 * {@code LoadState} document current state for {@code ChangeLoader}. It is in connection with
 * {@code LoadTask}. Each {@code LoadState} has three kinds of times.
 * <ul>
 * <li><B> produceTime </B> The time when Change Data Capture (CDC) produced the record. For each
 * origin table, LoadState documents the latest produceTime in the corresponding LoadTask.</li>
 * <li><B> receiveTime </B> The time when Bireme received the record. LoadState documents the
 * earliest receiveTime in the corresponding LoadTask.</li>
 * <li><B> completeTime </B> The time when ChangeLoader successfully loaded the LoadTask.</li>
 * </ul>
 *
 * @author yuze
 *
 */
public class LoadState {
  public HashMap<String, Long> produceTime;
  public HashMap<String, Long> receiveTime;
  public Long completeTime;

  public LoadState() {
    produceTime = new HashMap<String, Long>();
    receiveTime = new HashMap<String, Long>();
  }

  /**
   * Update the receiveTime, it document the earliest time.
   *
   * @param time Unix time
   */
  public void setReceiveTime(String table, Long time) {
    receiveTime.put(table, time);
  }

  /**
   * Set the completeTime when ChangeLoader has loaded the task.
   *
   * @param time Unix time
   */
  public void setCompleteTime(Long time) {
    completeTime = time;
  }

  /**
   * Set the produceTime for each origin table.
   *
   * @param table the name of origin table
   * @param time Unix time
   */
  public void setProduceTime(String table, Long time) {
    produceTime.put(table, time);
  }

  /**
   * Get the receiveTime.
   *
   * @return Unix time
   */
  public Long getReceiveTime(String table) {
    return receiveTime.get(table);
  }

  /**
   * Get the completeTime.
   *
   * @return Unix time
   */
  public Long getCompleteTime() {
    return completeTime;
  }

  /**
   * Get the produceTime for a specific table.
   *
   * @param table the name of origin table
   * @return Unix time
   */
  public Long getProduceTime(String table) {
    return produceTime.get(table);
  }
}
