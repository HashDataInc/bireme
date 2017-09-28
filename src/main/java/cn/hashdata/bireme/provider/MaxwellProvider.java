/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme.provider;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import cn.hashdata.bireme.BiremeException;
import cn.hashdata.bireme.BiremeUtility;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.Record;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.Table;
import cn.hashdata.bireme.Row.RowType;

/**
 * {@code MaxwellChangeProvider} is a type of {@code Provider} to process data from <B>Maxwell +
 * Kafka</B> data source.
 *
 * @author yuze
 *
 */
public class MaxwellProvider extends KafkaProvider {
  final public static String PROVIDER_TYPE = "Maxwell";

  public MaxwellProvider(Context cxt, KafkaProviderConfig config) {
    this(cxt, config, false);
  }

  /**
   * Create a new {@code MaxwellChangeProvider}.
   *
   * @param cxt bireme context
   * @param config {@code MaxwellConfig}.
   * @param test for unitest
   */
  public MaxwellProvider(Context cxt, KafkaProviderConfig config, boolean test) {
    super(cxt, config, test);
  }

  @Override
  protected ArrayList<TopicPartition> createTopicPartitions() {
    Iterator<PartitionInfo> iterator = consumer.partitionsFor(providerConfig.topic).iterator();
    ArrayList<TopicPartition> tpArray = new ArrayList<TopicPartition>();
    PartitionInfo partitionInfo;
    TopicPartition tp;

    while (iterator.hasNext()) {
      partitionInfo = iterator.next();
      tp = new TopicPartition(providerConfig.topic, partitionInfo.partition());
      tpArray.add(tp);
    }
    return tpArray;
  }

  @Override
  public Transformer createTransformer() {
    return new MaxwellChangeTransformer();
  }

  /**
   * {@code MaxwellChangeTransformer} is a type of {@code Transformer}. It is used to transform data
   * to {@code Row} from <B>Maxwell</B> data source.
   *
   * @author yuze
   *
   */
  public class MaxwellChangeTransformer extends KafkaTransformer {
    public class MaxwellRecord implements Record {
      public String dataSource;
      public String database;
      public String table;
      public RowType type;
      public JsonObject data;
      public JsonObject old;

      public MaxwellRecord(String changeValue) {
        JsonParser jsonParser = new JsonParser();
        JsonObject value = (JsonObject) jsonParser.parse(changeValue);

        this.dataSource = getProviderName();
        this.database = value.get("database").getAsString();
        this.table = value.get("table").getAsString();
        this.data = value.get("data").getAsJsonObject();

        if (value.has("old") && !value.get("old").isJsonNull()) {
          this.old = value.get("old").getAsJsonObject();
        }

        switch (value.get("type").getAsString()) {
          case "insert":
            type = RowType.INSERT;
            break;

          case "update":
            type = RowType.UPDATE;
            break;

          case "delete":
            type = RowType.DELETE;
            break;
        }
      }

      @Override
      public String getField(String fieldName, boolean oldValue) throws BiremeException {
        String field = null;

        if (oldValue) {
          try {
            field = BiremeUtility.jsonGetIgnoreCase(old, fieldName);
            return field;
          } catch (BiremeException ignore) {
          }
        }

        return BiremeUtility.jsonGetIgnoreCase(data, fieldName);
      }
    }

    public MaxwellChangeTransformer() {
      super();
    }

    private String getMappedTableName(MaxwellRecord record) {
      return tableMap.get(record.dataSource + "." + record.database + "." + record.table);
    }

    private String getOriginTableName(MaxwellRecord record) {
      return record.dataSource + "." + record.database + "." + record.table;
    }

    private boolean filter(MaxwellRecord record) {
      String fullTableName = record.dataSource + "." + record.database + "." + record.table;

      MaxwellProvider p = (MaxwellProvider) changeSet.provider;
      if (!p.providerConfig.tableMap.containsKey(fullTableName)) {
        // Do not sync this table
        return true;
      }

      return false;
    }

    @Override
    protected byte[] decodeToBinary(String data) {
      byte[] decoded = null;
      decoded = Base64.decodeBase64(data);
      return decoded;
    }

    @Override
    protected String decodeToBit(String data, int precision) {
      String binaryStr = Integer.toBinaryString(Integer.valueOf(data));
      return String.format("%" + precision + "s", binaryStr).replace(' ', '0');
    }

    @Override
    public boolean transform(ConsumerRecord<String, String> change, Row row)
        throws BiremeException {
      MaxwellRecord record = new MaxwellRecord(change.value());

      if (filter(record)) {
        return false;
      }

      Table table = cxt.tablesInfo.get(getMappedTableName(record));

      row.type = record.type;
      row.originTable = getOriginTableName(record);
      row.mappedTable = getMappedTableName(record);
      row.keys = formatColumns(record, table, table.keyIndexs, false);

      if (row.type == RowType.INSERT || row.type == RowType.UPDATE) {
        ArrayList<Integer> columns = new ArrayList<Integer>();

        for (int i = 0; i < table.ncolumns; ++i) {
          columns.add(i);
        }

        row.tuple = formatColumns(record, table, columns, false);
      }

      if (row.type == RowType.UPDATE) {
        row.oldKeys = formatColumns(record, table, table.keyIndexs, true);

        if (row.keys.equals(row.oldKeys)) {
          row.oldKeys = null;
        }
      }

      return true;
    }
  }
}
