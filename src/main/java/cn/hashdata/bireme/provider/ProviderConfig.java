package cn.hashdata.bireme.provider;

import java.util.HashMap;

public abstract class ProviderConfig {
  public enum SourceType { MAXWELL, DEBEZIUM }

  public String name;
  public SourceType type;
  public HashMap<String, String> tableMap;
}
