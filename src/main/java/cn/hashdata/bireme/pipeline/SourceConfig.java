/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme.pipeline;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * {@code SourceConfig} store the information about the source. {@link SourceConfig#tableMap}
 * maintain a kv set where key is the source table and value is the target table.
 * {@link SourceConfig#pipeLines} are the {@link PipeLine}s that belong to this source.
 *
 * @author yuze
 *
 */
public class SourceConfig {
  public enum SourceType { MAXWELL, DEBEZIUM }

  public String name;
  public SourceType type;
  public HashMap<String, String> tableMap;
  public ArrayList<PipeLine> pipeLines;

  // configuration for Kafka PipeLine
  public String topic;
  public String server;
  public String groupID;

  public SourceConfig(String name) {
    this.name = name;
    this.pipeLines = new ArrayList<PipeLine>();
  }
}
