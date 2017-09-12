package cn.hashdata.bireme;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

import cn.hashdata.bireme.Config;
import cn.hashdata.bireme.Table;
import cn.hashdata.bireme.Config.ConnectionConfig;
import cn.hashdata.bireme.provider.KafkaProviderConfig;

public class TestUtil {
  public static void addMaxellDataSource(Config conf, int tableCount) {
    String name = "datasource_" + conf.dataSource.size();
    KafkaProviderConfig maxwellConfig = new KafkaProviderConfig();
    maxwellConfig.name = name;
    maxwellConfig.server = "127.0.0.1:9092";
    maxwellConfig.topic = name;
    maxwellConfig.tableMap = new HashMap<String, String>();
    for (int count = 0; count < tableCount; count++) {
      maxwellConfig.tableMap.put(name + "."
              + "demo.table" + count,
          "public." + name + "_table_" + count);
      conf.tableMap.put(name + "."
              + "demo.table" + count,
          "public." + name + "_table_" + count);
      conf.loadersCount++;
    }

    conf.maxwellConf.add(maxwellConfig);
    conf.dataSource.add(name);
    conf.dataSourceType.add("maxwell");
  }

  public static Config generateConfig() {
    Config conf = new Config();
    ConnectionConfig connectionConfig = new ConnectionConfig();
    connectionConfig.jdbcUrl = "jdbc:postgresql://127.0.0.1:5432/postgres";
    connectionConfig.user = "postgres";
    connectionConfig.passwd = "postgres";
    conf.target = connectionConfig;
    conf.bookkeeping = connectionConfig;
    return conf;
  }

  public static Table generateTableInfo(ArrayList<Integer> types, ArrayList<Integer> keyIndexs) {
    Table table = new Table();
    table.ncolumns = types.size();
    for (int i = 0; i < types.size(); i++) {
      table.columnName.add("column" + i);
    }
    for (int type : types) {
      switch (type) {
        case Types.CHAR:
        case Types.NCHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
          table.columnType.add(Types.VARCHAR);
          table.columnPrecision.add(20);
          break;

        case Types.BINARY:
        case Types.BLOB:
        case Types.CLOB:
        case Types.LONGVARBINARY:
        case Types.NCLOB:
        case Types.VARBINARY:
          table.columnType.add(Types.VARBINARY);
          table.columnPrecision.add(20);
          break;

        case Types.BIT:
          table.columnType.add(Types.BIT);
          table.columnPrecision.add(10);
          break;

        default:
          table.columnType.add(Types.INTEGER);
          table.columnPrecision.add(10);
          break;
      }
      table.columnScale.add(0);
    }

    for (int index : keyIndexs) {
      table.keyNames.add("column" + index);
      table.keyIndexs.add(index);
    }
    return table;
  }
}
