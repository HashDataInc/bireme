package cn.hashdata.dbsync.provider;

import java.util.HashMap;

import cn.hashdata.dbsync.provider.KafkaProvider.SourceType;

public class KafkaProviderConfig {
  public String name;
  public SourceType type;
  public String topic;
  public String server;
  public HashMap<String, String> tableMap;
}
