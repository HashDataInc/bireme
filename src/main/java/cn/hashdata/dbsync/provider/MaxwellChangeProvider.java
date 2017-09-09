package cn.hashdata.dbsync.provider;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import cn.hashdata.dbsync.Context;
import cn.hashdata.dbsync.DbsyncException;
import cn.hashdata.dbsync.Record;
import cn.hashdata.dbsync.Row;
import cn.hashdata.dbsync.Row.RowType;
import cn.hashdata.dbsync.Table;

/**
 * {@code MaxwellChangeProvider} is a type of {@code Provider} to process data from <B>Maxwell +
 * Kafka</B> data source.
 *
 * @author yuze
 *
 */
public class MaxwellChangeProvider extends KafkaProvider {
  final public static String PROVIDER_TYPE = "Maxwell";

  /**
   * Create a new {@code MaxwellChangeProvider}.
   *
   * @param cxt dbsync context
   * @param config {@code MaxwellConfig}.
   */
  public MaxwellChangeProvider(Context cxt, KafkaProviderConfig config) {
    super(cxt, config);
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

  @Override
  public String getProviderType() {
    return PROVIDER_TYPE + ABSTRACT_PROVIDER_TYPE;
  }

  /**
   * {@code MaxwellChangeTransformer} is a type of {@code Transformer}. It is used to transform data
   * to {@code Row} from <B>Maxwell</B> data source.
   *
   * @author yuze
   *
   */
  public class MaxwellChangeTransformer extends KafkaTransformer {
    protected Gson gson;

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
      public String getField(String fieldName, boolean oldValue) {
        JsonElement element = null;
        String field = null;

        if (oldValue && old.has(fieldName)) {
          element = old.get(fieldName);
        } else {
          element = data.get(fieldName);
        }

        if (!element.isJsonNull()) {
          field = element.getAsString();
        }

        return field;
      }
    }

    public MaxwellChangeTransformer() {
      super();
      this.gson = new Gson();
    }

    private String formatTuple(MaxwellRecord record, Table table) {
      ArrayList<Integer> columns = new ArrayList<Integer>();

      for (int i = 0; i < table.ncolumns; ++i) {
        columns.add(i);
      }

      return formatColumns(record, table, columns, false);
    }

    private String formatKeys(MaxwellRecord record, Table table, boolean oldKey) {
      return formatColumns(record, table, table.keyIndexs, oldKey);
    }

    private String getMappedTableName(MaxwellRecord record) {
      return tableMap.get(record.dataSource + "." + record.database + "." + record.table);
    }

    private String getOriginTableName(MaxwellRecord record) {
      return record.dataSource + "." + record.database + "." + record.table;
    }

    private boolean filter(MaxwellRecord record) {
      String fullTableName = record.dataSource + "." + record.database + "." + record.table;

      MaxwellChangeProvider p = (MaxwellChangeProvider) changeSet.provider;
      if (!p.providerConfig.tableMap.containsKey(fullTableName)) {
        // Do not sync this table
        return true;
      }

      return false;
    }

    /**
     * Convert {@code MaxwellRecord} into {@code Row}.
     *
     * @param record {@code MaxwellRecord} from Maxwell, which is extracted from change data
     * @param type insert, update or delete
     * @return the converted row
     * @throws DbsyncException Exception while borrow from pool
     */
    public Row convertRecord(MaxwellRecord record, RowType type) throws DbsyncException {
      Table table = cxt.tablesInfo.get(getMappedTableName(record));
      Row row = null;
      try {
        row = cxt.idleRows.borrowObject();
      } catch (Exception e) {
        new DbsyncException(e.getCause());
      }

      row.type = type;
      row.originTable = getOriginTableName(record);
      row.mappedTable = getMappedTableName(record);
      row.keys = formatKeys(record, table, false);

      if (type == RowType.INSERT) {
        row.tuple = formatTuple(record, table);
      } else if (type == RowType.UPDATE) {
        row.tuple = formatTuple(record, table);
        row.oldKeys = formatKeys(record, table, true);

        if (row.keys.equals(row.oldKeys)) {
          row.oldKeys = null;
        }
      }

      return row;
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
    public boolean transform(ConsumerRecord<String, String> change, Row row) {
      MaxwellRecord record = new MaxwellRecord(change.value());

      if (filter(record)) {
        return false;
      }

      Table table = cxt.tablesInfo.get(getMappedTableName(record));

      row.type = record.type;
      row.originTable = getOriginTableName(record);
      row.mappedTable = getMappedTableName(record);
      row.keys = formatKeys(record, table, false);

      if (row.type == RowType.INSERT) {
        row.tuple = formatTuple(record, table);
      } else if (row.type == RowType.UPDATE) {
        row.tuple = formatTuple(record, table);
        row.oldKeys = formatKeys(record, table, true);

        if (row.keys.equals(row.oldKeys)) {
          row.oldKeys = null;
        }
      }

      return true;
    }
  }
}
