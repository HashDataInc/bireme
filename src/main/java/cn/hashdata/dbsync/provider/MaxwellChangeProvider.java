package cn.hashdata.dbsync.provider;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import cn.hashdata.dbsync.ChangeSet;
import cn.hashdata.dbsync.Transformer;
import cn.hashdata.dbsync.Config.MaxwellConfig;
import cn.hashdata.dbsync.Context;
import cn.hashdata.dbsync.DbsyncException;
import cn.hashdata.dbsync.Position;
import cn.hashdata.dbsync.Provider;
import cn.hashdata.dbsync.Row;
import cn.hashdata.dbsync.Row.RowType;
import cn.hashdata.dbsync.RowSet;
import cn.hashdata.dbsync.Table;

/**
 * {@code MaxwellChangeProvider} is a type of {@code Provider} to process data from <B>Maxwell +
 * Kafka</B> data source.
 *
 * @author yuze
 *
 */
public class MaxwellChangeProvider implements Callable<Long>, Provider {
  static final protected Long TIMEOUT_MS = 1000L;
  static final public String PROVIDER_TYPE = "Maxwell";

  private Logger logger = LogManager.getLogger("Dbsync." + MaxwellChangeProvider.class);
  private Meter providerMeter;

  protected Context cxt;
  protected LinkedBlockingQueue<ChangeSet> changeSetOut;
  protected ArrayList<TopicPartition> tpArray;
  protected Properties props;
  protected KafkaConsumer<String, String> consumer;
  protected MaxwellConfig providerConfig;
  private LinkedBlockingQueue<Transformer> idleTransformer;

  /**
   * Create a new {@code MaxwellChangeProvider}.
   *
   * @param cxt dbsync context
   * @param config {@code MaxwellConfig}.
   * @throws DbsyncException - wrap and throw Exception which cannot be handled
   */
  public MaxwellChangeProvider(Context cxt, MaxwellConfig config) throws DbsyncException {
    this(cxt, config, false);
  }

  /**
   * Create a new {@code MaxwellChangeProvider}.
   *
   * @param cxt dbsync context
   * @param config {@code MaxwellConfig}.
   * @param test unitest or not
   * @throws DbsyncException - wrap and throw Exception which cannot be handled
   */
  public MaxwellChangeProvider(Context cxt, MaxwellConfig config, Boolean test)
      throws DbsyncException {
    this.cxt = cxt;
    this.changeSetOut = cxt.changeSetQueue;
    this.props = new Properties();
    this.providerConfig = config;
    this.idleTransformer = new LinkedBlockingQueue<Transformer>();
    this.tpArray = new ArrayList<TopicPartition>();

    if (!test) {
      setupKafkaConsumer();

      this.providerMeter =
          cxt.metrics.meter(MetricRegistry.name(MaxwellChangeProvider.class, providerConfig.name));

      for (Entry<String, String> entry : providerConfig.tableMap.entrySet()) {
        logger.info("MaxWellChangeProvider {}: Sync {} to {}.", providerConfig.name, entry.getKey(),
            entry.getValue());
      }
    }
  }

  private void setupKafkaConsumer() throws DbsyncException {
    SetProps(this.props, providerConfig);
    this.consumer = new KafkaConsumer<String, String>(props);
    Iterator<PartitionInfo> iterator = consumer.partitionsFor(providerConfig.topic).iterator();

    PartitionInfo partitionInfo;
    TopicPartition tp;
    while (iterator.hasNext()) {
      partitionInfo = iterator.next();
      tp = new TopicPartition(providerConfig.topic, partitionInfo.partition());
      tpArray.add(tp);
    }
    consumer.assign(tpArray);

    HashMap<Integer, Long> partitionOffset = new HashMap<Integer, Long>();
    for (TopicPartition topicPartition : tpArray) {
      partitionOffset.put(topicPartition.partition(), Long.MAX_VALUE);
    }

    for (String table : providerConfig.tableMap.keySet()) {
      if (cxt.bookkeeping.containsKey(table)) {
        Position p = cxt.bookkeeping.get(table).getLeft();

        if (p.getType() != PROVIDER_TYPE) {
          String message = "Provider type for table " + table + " does not match privious!";
          logger.fatal(message);
          throw new DbsyncException(message);
        }

        Integer partition = ((MaxwellChangePosition) p).partition;
        Long offset = ((MaxwellChangePosition) p).offset;
        if (offset < partitionOffset.get(partition)) {
          partitionOffset.put(partition, offset);
        }
      }
    }

    for (TopicPartition topicPartition : tpArray) {
      Integer partition = topicPartition.partition();
      Long offset = partitionOffset.get(partition);
      offset = offset == Long.MAX_VALUE ? 0 : offset;

      consumer.seek(topicPartition, offset);
      logger.info("MaxwellChangeProvider {}: partition {}, offset {}.", providerConfig.name,
          partition, offset);
    }
  }

  /**
   * Call the {@code MaxwellChangeProvider} to work.
   */
  @Override
  public Long call() throws InterruptedException, Exception {
    System.out.println(Thread.currentThread());
    Thread.currentThread().setName("MaxWellChangeProvider: " + providerConfig.name);

    logger.info("MaxwellChangeProvider {} start. Server: {}, Topic: {}.", providerConfig.name,
        providerConfig.server, providerConfig.topic);

    ConsumerRecords<String, String> records;
    ChangeSet changeSet;

    try {
      while (!cxt.stop) {
        do {
          records = consumer.poll(TIMEOUT_MS);
        } while (records.isEmpty() && !cxt.stop);

        if (cxt.stop) {
          break;
        }

        providerMeter.mark(records.count());

        changeSet = cxt.idleChangeSets.borrowObject();
        changeSet.provider = this;
        changeSet.createdAt = new Date();
        changeSet.changes = records;

        boolean success;
        do {
          success = changeSetOut.offer(changeSet, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } while (!success && !cxt.stop);

        if (success) {
          logger.trace("{} provide new changeSet {}, #records: {}.", providerConfig.name,
              changeSet.hashCode(), records.count());
        }
      }
    } finally {
      try {
        consumer.close();
      } catch (Exception ignore) {
      }
    }

    logger.info("MaxwellChangeProvider {} exit. Server: {}, Topic: {}.", providerConfig.name,
        providerConfig.server, providerConfig.topic);

    return 0L;
  }

  private void SetProps(Properties props, MaxwellConfig conf) {
    props.put("bootstrap.servers", conf.server);
    props.put("group.id", "dbsync");
    props.put("enable.auto.commit", true);
    props.put("auto.commit.interval.ms", 1000);
    props.put("session.timeout.ms", 30000);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest");
  }

  @Override
  public Transformer borrowTransformer(ChangeSet changeSet) {
    Transformer transformer = idleTransformer.poll();

    if (transformer == null) {
      transformer = new MaxwellChangeTransformer(cxt);
    }

    transformer.setChangeSet(changeSet);
    return transformer;
  }

  @Override
  public void returnTransformer(Transformer trans) {
    trans.setChangeSet(null);
    idleTransformer.offer(trans);
  }

  @Override
  public String getProviderName() {
    return providerConfig.name;
  }

  /**
   * {@code MaxwellChangePosition} is a type of {@code Position}. It is used to mark the offset of
   * data in <B>Maxwell</B> data source.
   *
   * @author yuze
   *
   */
  public static class MaxwellChangePosition implements Position {
    public Integer partition;
    public Long offset;
    public String type;

    public MaxwellChangePosition(Integer partition, Long offset) {
      this.partition = partition;
      this.offset = offset;
      this.type = PROVIDER_TYPE;
    }

    public MaxwellChangePosition(String partitionOffset) {
      fromString(partitionOffset);
      this.type = PROVIDER_TYPE;
    }

    @Override
    public String toStirng() {
      return Integer.toString(partition) + ":" + Long.toString(offset);
    }

    @Override
    public void fromString(String str) {
      String[] arg = str.split(":");
      partition = Integer.valueOf(arg[0]);
      offset = Long.valueOf(arg[1]);
    }

    @Override
    public boolean lessEqual(Position other) throws DbsyncException {
      if (!this.type.equals(other.getType())) {
        String message = "Position type does not match the previous";
        throw new DbsyncException(message);
      }

      MaxwellChangePosition o = (MaxwellChangePosition) other;
      return offset <= o.offset;
    }

    @Override
    public String getType() {
      return type;
    }
  }

  /**
   * {@code MaxwellChangeTransformer} is a type of {@code Transformer}. It is used to transform data
   * to {@code Row} from <B>Maxwell</B> data source.
   *
   * @author yuze
   *
   */
  public static class MaxwellChangeTransformer extends Transformer {
    private static final char FIELD_DELIMITER = '|';
    private static final char NEWLINE = '\n';
    private static final char QUOTE = '"';
    private static final char ESCAPE = '\\';

    private Logger logger = LogManager.getLogger("Dbsync." + MaxwellChangeTransformer.class);

    protected Context cxt;
    protected HashMap<String, String> tableMap;
    protected StringBuilder tupleStringBuilder;
    protected StringBuilder fieldStringBuilder;
    protected Gson gson;

    public static class Record {
      public String dataSource;
      public String database;
      public String table;
      public String type;
      public Long ts;
      public Long xid;
      public boolean commit;
      public JsonObject data;
      public JsonObject old;
      public Position position;
    }

    public MaxwellChangeTransformer(Context cxt) {
      this.cxt = cxt;
      this.tableMap = cxt.tableMap;
      this.tupleStringBuilder = new StringBuilder();
      this.fieldStringBuilder = new StringBuilder();
      this.gson = new Gson();
    }

    @SuppressWarnings("unchecked")
    @Override
    public RowSet call() throws Exception {
      Thread.currentThread().setName("MaxwellChangeTransformer");

      RowSet set = cxt.idleRowSets.borrowObject();
      set.createdAt = changeSet.createdAt;
      set.rowBucket.clear();

      int count = 0;
      Record record;

      for (ConsumerRecord<String, String> change :
          (ConsumerRecords<String, String>) changeSet.changes) {
        record = gson.fromJson(change.value(), Record.class);
        record.dataSource = provider.getProviderName();
        record.position = new MaxwellChangePosition(change.partition(), change.offset());
        // filter
        if (filter(record)) {
          continue;
        }
        // transform
        count++;
        switch (record.type) {
          case "insert":
            addToRowSet(set, convertRecord(record, RowType.INSERT));
            break;
          case "delete":
            addToRowSet(set, convertRecord(record, RowType.DELETE));
            break;
          case "update":
            addToRowSet(set, convertRecord(record, RowType.UPDATE));
            break;
        }
      }

      logger.trace("Transform ChangeSet {} to RowSet {}, #rows: {}.", changeSet.hashCode(),
          set.hashCode(), count);

      cxt.idleChangeSets.returnObject(changeSet);
      return set;
    }

    private String escapeString(String data) {
      fieldStringBuilder.setLength(0);

      for (int i = 0; i < data.length(); ++i) {
        char c = data.charAt(i);

        switch (c) {
          case 0x00:
            logger.warn("illegal character 0x00, deleted.");
            continue;
          case QUOTE:
          case ESCAPE:
            fieldStringBuilder.append(ESCAPE);
        }

        fieldStringBuilder.append(c);
      }

      return fieldStringBuilder.toString();
    }

    private String escapeBinary(byte[] data) {
      fieldStringBuilder.setLength(0);

      for (int i = 0; i < data.length; ++i) {
        if (data[i] == '\\') {
          fieldStringBuilder.append('\\');
          fieldStringBuilder.append('\\');
        } else if (data[i] < 0x20 || data[i] > 0x7e) {
          byte b = data[i];
          char[] val = new char[3];
          val[2] = (char) ((b & 07) + '0');
          b >>= 3;
          val[1] = (char) ((b & 07) + '0');
          b >>= 3;
          val[0] = (char) ((b & 03) + '0');
          fieldStringBuilder.append('\\');
          fieldStringBuilder.append(val);
        } else {
          fieldStringBuilder.append((char) (data[i]));
        }
      }

      return fieldStringBuilder.toString();
    }

    private String formatColumns(Record record, Table table, ArrayList<Integer> columns,
        boolean oldValue) throws DbsyncException {
      tupleStringBuilder.setLength(0);

      for (int i = 0; i < columns.size(); ++i) {
        int columnIndex = columns.get(i);
        JsonElement element = null;
        String data = null;
        String columnName = table.columnName.get(columnIndex);

        if (oldValue && record.old.has(columnName)) {
          element = record.old.get(columnName);
        } else {
          element = record.data.get(columnName);
        }

        if (element.isJsonNull()) {
          data = null;
        } else {
          data = element.getAsString();
        }

        switch (table.columnType.get(columnIndex)) {
          case Types.CHAR:
          case Types.NCHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.NVARCHAR:
          case Types.LONGNVARCHAR: {
            if (data != null) {
              tupleStringBuilder.append(QUOTE);
              tupleStringBuilder.append(escapeString(data));
              tupleStringBuilder.append(QUOTE);
            }
            break;
          }

          case Types.BINARY:
          case Types.BLOB:
          case Types.CLOB:
          case Types.LONGVARBINARY:
          case Types.NCLOB:
          case Types.VARBINARY: {
            if (data != null) {
              byte[] decoded = null;
              try {
                decoded = Base64.decodeBase64(data);
              } catch (Exception e) {
                throw new DbsyncException(e.getMessage());
              }

              tupleStringBuilder.append(escapeBinary(decoded));
            }
            break;
          }

          case Types.BIT: {
            if (data != null) {
              int precision = table.columnPrecision.get(columnIndex);
              String binaryStr = Integer.toBinaryString(Integer.valueOf(data));
              tupleStringBuilder.append(
                  String.format("%" + precision + "s", binaryStr).replace(' ', '0'));
            }

            break;
          }
          default: {
            if (data != null) {
              tupleStringBuilder.append(data);
            }
            break;
          }
        }

        if (i + 1 < columns.size()) {
          tupleStringBuilder.append(FIELD_DELIMITER);
        }
      }
      tupleStringBuilder.append(NEWLINE);

      return tupleStringBuilder.toString();
    }

    private String formatTuple(Record record, Table table) throws DbsyncException {
      ArrayList<Integer> columns = new ArrayList<Integer>();

      for (int i = 0; i < table.ncolumns; ++i) {
        columns.add(i);
      }

      return formatColumns(record, table, columns, false);
    }

    private String formatKeys(Record record, Table table, boolean oldKey) throws DbsyncException {
      return formatColumns(record, table, table.keyIndexs, oldKey);
    }

    private String getMappedTableName(Record record) {
      return tableMap.get(record.dataSource + "." + record.database + "." + record.table);
    }

    private String getOriginTableName(Record record) {
      return record.dataSource + "." + record.database + "." + record.table;
    }

    private void addToRowSet(RowSet set, Row row) throws InterruptedException, Exception {
      HashMap<String, ArrayList<Row>> bucket = set.rowBucket;
      String mappedTable = row.mappedTable;
      ArrayList<Row> array = bucket.get(mappedTable);

      if (array == null) {
        array = cxt.idleRowArrays.borrowObject();
        bucket.put(mappedTable, array);
      }

      array.add(row);
    }

    private Boolean filter(Record record) throws DbsyncException {
      String fullTableName = record.dataSource + "." + record.database + "." + record.table;
      ConcurrentHashMap<String, Pair<Position, String>> bookKeeping = cxt.bookkeeping;
      MaxwellChangePosition prePosition;

      MaxwellChangeProvider p = (MaxwellChangeProvider) changeSet.provider;
      if (!p.providerConfig.tableMap.containsKey(fullTableName)) {
        // Do not sync this table
        return true;
      }

      if (bookKeeping.get(fullTableName) == null) {
        // sync this table but haven't bookKeeped
        return false;
      }

      if (bookKeeping.get(fullTableName).getRight().equals("Error")) {
        // the loader for this table is stop
        return true;
      }

      prePosition = (MaxwellChangePosition) bookKeeping.get(fullTableName).getLeft();
      return record.position.lessEqual(prePosition);
    }

    public Row convertRecord(Record record, RowType type) throws DbsyncException, Exception {
      Table table = cxt.tablesInfo.get(getMappedTableName(record));
      Row row = cxt.idleRows.borrowObject();

      row.type = type;
      row.originTable = getOriginTableName(record);
      row.mappedTable = getMappedTableName(record);
      row.position = record.position;
      record.position = null;
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
  }
}
