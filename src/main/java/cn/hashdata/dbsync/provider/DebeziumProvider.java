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
import cn.hashdata.dbsync.Row;
import cn.hashdata.dbsync.Row.RowType;
import cn.hashdata.dbsync.Table;

/**
 * {@code DebeziumProvider} is a type of {@code Provider} to process data from <B>Debezium +
 * Kafka</B> data source.
 *
 * @author yuze
 *
 */
public class DebeziumProvider extends KafkaProvider {
  final public static String PROVIDER_TYPE = "Debezium";

  public DebeziumProvider(Context cxt, KafkaProviderConfig config) {
    super(cxt, config);
  }

  @Override
  protected ArrayList<TopicPartition> createTopicPartitions() {
    Iterator<PartitionInfo> iterator;
    ArrayList<TopicPartition> tpArray = new ArrayList<TopicPartition>();
    PartitionInfo partitionInfo;
    TopicPartition tp;

    for (String topic : providerConfig.tableMap.keySet()) {
      iterator = consumer.partitionsFor(topic).iterator();

      while (iterator.hasNext()) {
        partitionInfo = iterator.next();
        tp = new TopicPartition(topic, partitionInfo.partition());
        tpArray.add(tp);
      }
    }

    return tpArray;
  }

  @Override
  public Transformer createTransformer() {
    return new DebeziumTransformer();
  }

  @Override
  public String getProviderType() {
    return PROVIDER_TYPE + ABSTRACT_PROVIDER_TYPE;
  }

  public class DebeziumTransformer extends KafkaTransformer {
    protected Gson gson;

    public class DebeziumRecord implements Record {
      public String topic;
      public RowType type;
      public JsonObject data;

      public DebeziumRecord(String topic, JsonObject payLoad) {
        this.topic = topic;
        char op = payLoad.get("op").getAsCharacter();

        JsonElement element = null;
        switch (op) {
          case 'c':
            type = RowType.INSERT;
            element = payLoad.get("after");
            break;
          case 'u':
            type = RowType.UPDATE;
            element = payLoad.get("after");
            break;
          case 'd':
            type = RowType.DELETE;
            element = payLoad.get("before");
            break;
        }

        this.data = element.getAsJsonObject();
      }

      @Override
      public String getField(String fieldName, boolean oldValue) {
        JsonElement element = null;
        String field = null;

        element = data.get(fieldName);
        if (!element.isJsonNull()) {
          field = element.getAsString();
        }

        return field;
      }
    }

    public DebeziumTransformer() {
      this.tupleStringBuilder = new StringBuilder();
      this.fieldStringBuilder = new StringBuilder();
      this.gson = new Gson();
    }

    private String formatTuple(DebeziumRecord record, Table table) {
      ArrayList<Integer> columns = new ArrayList<Integer>();

      for (int i = 0; i < table.ncolumns; ++i) {
        columns.add(i);
      }

      return formatColumns(record, table, columns, false);
    }

    private String formatKeys(DebeziumRecord record, Table table, boolean oldKey) {
      return formatColumns(record, table, table.keyIndexs, oldKey);
    }

    private String getMappedTableName(DebeziumRecord record) {
      return tableMap.get(record.topic);
    }

    private String getOriginTableName(DebeziumRecord record) {
      return record.topic;
    }

    /**
     * Convert Record from debezium to dbsync internal format.
     *
     * @param record {@code Record} from debezium
     * @param type insert, update or delete
     * @return the converted row
     * @throws DbsyncException - Exception while borrow from pool
     */
    public Row convertRecord(DebeziumRecord record, RowType type) throws DbsyncException {
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

      if (type != RowType.DELETE) {
        row.tuple = formatTuple(record, table);
      }

      return row;
    }

    @Override
    public Row transform(ConsumerRecord<String, String> change) {
      JsonParser jsonParser = new JsonParser();
      JsonObject value = (JsonObject) jsonParser.parse(change.value());

      if (!value.has("payload") || value.get("payload").isJsonNull()) {
        return null;
      }

      JsonObject payLoad = value.getAsJsonObject("payload");
      DebeziumRecord record = new DebeziumRecord(change.topic(), payLoad);

      Table table = cxt.tablesInfo.get(getMappedTableName(record));
      Row row = null;

      try {
        row = cxt.idleRows.borrowObject();
      } catch (Exception e) {
        String message = "Can't not borrow RowSet from the Object Pool.";
        new DbsyncException(message, e);
      }

      row.type = record.type;
      row.originTable = getOriginTableName(record);
      row.mappedTable = getMappedTableName(record);
      row.keys = formatKeys(record, table, false);

      if (row.type != RowType.DELETE) {
        row.tuple = formatTuple(record, table);
      }

      return row;
    }

    @Override
    protected byte[] decodeString(String data) {
      // TODO
      byte[] decoded = null;
      decoded = Base64.decodeBase64(data);
      return decoded;
    }
  }
}
