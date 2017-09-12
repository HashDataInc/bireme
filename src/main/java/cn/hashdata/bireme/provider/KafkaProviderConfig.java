package cn.hashdata.bireme.provider;

import java.util.HashMap;

import cn.hashdata.bireme.provider.KafkaProvider.SourceType;

public class KafkaProviderConfig {
  public String name;
  public SourceType type;
  public String topic;
  public String server;
  public HashMap<String, String> tableMap;
}
