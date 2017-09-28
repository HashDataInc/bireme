package cn.hashdata.bireme;

import java.util.HashMap;

public class LoadStatus {
  public HashMap<String, Long> produceTime;
  public Long receiveTime = Long.MAX_VALUE;
  public Long completeTime;

  public LoadStatus() {
    produceTime = new HashMap<String, Long>();
  }

  public void setReceiveTime(Long time) {
    receiveTime = time < receiveTime ? time : receiveTime;
  }

  public void setCompleteTime(Long time) {
    completeTime = time;
  }

  public void setProduceTime(String table, Long time) {
    produceTime.put(table, time);
  }

  public Long getReceiveTime() {
    return receiveTime;
  }

  public Long getCompleteTime() {
    return completeTime;
  }

  public Long getProduceTime(String table) {
    return produceTime.get(table);
  }
}
