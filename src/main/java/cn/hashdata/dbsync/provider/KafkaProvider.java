package cn.hashdata.dbsync.provider;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import cn.hashdata.dbsync.AbstractCommitCallback;
import cn.hashdata.dbsync.ChangeSet;
import cn.hashdata.dbsync.CommitCallback;
import cn.hashdata.dbsync.Context;
import cn.hashdata.dbsync.DbsyncException;
import cn.hashdata.dbsync.Provider;
import cn.hashdata.dbsync.Row;
import cn.hashdata.dbsync.RowSet;

public abstract class KafkaProvider extends Provider {
  protected static final Long TIMEOUT_MS = 1000L;
  public static final String ABSTRACT_PROVIDER_TYPE = "Kafka";

  public enum SourceType { MAXWELL, DEBEZIUM }

  public KafkaProviderConfig providerConfig;
  protected KafkaConsumer<String, String> consumer;
  private LinkedBlockingQueue<KafkaCommitCallback> commitCallbacks;

  public KafkaProvider(Context cxt, KafkaProviderConfig providerConfig) {
    super(cxt);
    this.providerConfig = providerConfig;
    this.commitCallbacks = new LinkedBlockingQueue<KafkaCommitCallback>();

    consumer = new KafkaConsumer<String, String>(kafkaProps());
    consumer.assign(createTopicPartitions());
  }

  @Override
  public String getProviderName() {
    return providerConfig.name;
  }

  private void checkAndCommit() {
    CommitCallback callback = null;

    while (!commitCallbacks.isEmpty()) {
      if (commitCallbacks.peek().ready()) {
        callback = commitCallbacks.remove();
      } else {
        break;
      }
    }

    if (callback != null) {
      callback.commit();
    }
  }

  private Properties kafkaProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", providerConfig.server);
    props.put("group.id", "dbsync22");
    props.put("enable.auto.commit", false);
    props.put("session.timeout.ms", 30000);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest");
    return props;
  }

  private ChangeSet packRecords(ConsumerRecords<String, String> records,
      KafkaCommitCallback callback) throws DbsyncException {
    ChangeSet changeSet;

    try {
      changeSet = cxt.idleChangeSets.borrowObject();
      changeSet.provider = this;
      changeSet.createdAt = new Date();
      changeSet.changes = records;
      changeSet.callback = callback;
    } catch (Exception e) {
      String message = "Can't not borrow ChangeSet from the Object Pool.";
      throw new DbsyncException(message, e);
    }

    return changeSet;
  }

  protected abstract ArrayList<TopicPartition> createTopicPartitions();

  @Override
  public Long call() throws DbsyncException, InterruptedException {
    ConsumerRecords<String, String> records = null;
    ChangeSet changeSet = null;
    boolean success = false;

    try {
      while (!cxt.stop) {
        do {
          records = consumer.poll(TIMEOUT_MS);
          checkAndCommit();
        } while (records.isEmpty() && !cxt.stop);

        if (cxt.stop) {
          break;
        }

        KafkaCommitCallback callback = new KafkaCommitCallback();

        if (!commitCallbacks.offer(callback)) {
          String Message = "Can't add CommitCallback to queue.";
          throw new DbsyncException(Message);
        }

        changeSet = packRecords(records, callback);

        do {
          success = changeSetOut.offer(changeSet, TIMEOUT_MS, TimeUnit.MILLISECONDS);
          checkAndCommit();
        } while (!success && !cxt.stop);
      }
    } finally {
      try {
        consumer.close();
      } catch (Exception ignore) {
      }
    }

    return 0L;
  }

  public abstract class KafkaTransformer extends Transformer {
    @SuppressWarnings("unchecked")
    @Override
    public void fillRowSet(RowSet rowSet) throws DbsyncException {
      CommitCallback callback = changeSet.callback;
      HashMap<TopicPartition, Long> offsets = ((KafkaCommitCallback) callback).partitionOffset;
      Row row = null;

      for (ConsumerRecord<String, String> change :
          (ConsumerRecords<String, String>) changeSet.changes) {
        try {
          row = cxt.idleRows.borrowObject();
        } catch (Exception e) {
          String message = "Can't not borrow RowSet from the Object Pool.";
          new DbsyncException(message, e);
        }

        if (transform(change, row) == null) {
          cxt.idleRows.returnObject(row);
          continue;
        }

        addToRowSet(row, rowSet);
        offsets.put(new TopicPartition(change.topic(), change.partition()), change.offset());
      }

      callback.setNumOfTables(rowSet.rowBucket.size());
      rowSet.callback = callback;
    }

    public abstract Row transform(ConsumerRecord<String, String> change, Row row);
  }

  public class KafkaCommitCallback extends AbstractCommitCallback {
    public HashMap<TopicPartition, Long> partitionOffset;

    public KafkaCommitCallback() {
      this.partitionOffset = new HashMap<TopicPartition, Long>();
    }

    @Override
    public String getType() {
      return null;
    }

    @Override
    public String toStirng() {
      return null;
    }

    @Override
    public void fromString(String str) {}

    @Override
    public void commit() {
      HashMap<TopicPartition, OffsetAndMetadata> offsets =
          new HashMap<TopicPartition, OffsetAndMetadata>();
      partitionOffset.forEach(
          (key, value) -> { offsets.put(key, new OffsetAndMetadata(value + 1)); });

      consumer.commitSync(offsets);
      committed.set(true);
      partitionOffset.clear();
    }
  }
}
