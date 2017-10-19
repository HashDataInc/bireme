/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme.provider;

import java.util.HashMap;

public class SourceConfig {
  public enum SourceType { MAXWELL, DEBEZIUM }

  public String name;
  public SourceType type;
  public HashMap<String, String> tableMap;

  // for Kafka PipeLine
  public String topic;
  public String server;
  public String groupID;

  public SourceConfig(String name) {
    this.name = name;
  }
}
