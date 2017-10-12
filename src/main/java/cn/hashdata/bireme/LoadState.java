package cn.hashdata.bireme;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * {@code LoadState} document current state for {@code ChangeLoader}. It is in connection with
 * {@code LoadTask}. Each {@code LoadState} has three kinds of times.
 * <ul>
 * <li><B> produceTime </B> The time when Change Data Capture (CDC) produced the record. For each
 * origin table, LoadState documents the latest produceTime in the corresponding LoadTask.</li>
 * <li><B> receiveTime </B> The time when Bireme received the record. LoadState documents the
 * earliest receiveTime for each origin table in the corresponding LoadTask.</li>
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
   * Set the receiveTime for each origin table.
   *
   * @param table the name of origin table
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
   * Get the receiveTime for a specific table
   *
   * @param table the name of origin table
   * @return UnixTime
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

  /**
   * Reorganized the State into an object that is convenient to form a Json String.
   * 
   * @param targetTable the target Table
   * @return A State object
   */
  public PlainState getPlainState(String targetTable) {
    PlainState srcState = new PlainState(targetTable, new Date(completeTime));

    for (Entry<String, Long> iter : produceTime.entrySet()) {
      Source src = new Source();
      String originTable = iter.getKey();
      Long produceTime = iter.getValue();
      Long receiveTime = getReceiveTime(originTable);

      String[] split = iter.getKey().split("\\.", 2);

      src.source = split[0];
      src.table_name = split[1];
      src.produce_time = new Date(produceTime);
      src.receive_time = new Date(receiveTime);

      srcState.sources.add(src);
    }

    return srcState;
  }

  /**
   * PlainState is a class which is convenient to form a Json String.
   * 
   * @author yuze
   *
   */
  public class PlainState {
    String target_table;
    ArrayList<Source> sources;
    Date complete_time;

    public PlainState(String targetTable, Date completeTime) {
      target_table = targetTable;
      sources = new ArrayList<Source>();
      complete_time = completeTime;
    }
  }
  class Source {
    String source;
    String table_name;
    Date produce_time;
    Date receive_time;
  }
}
