/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme.provider;

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

import cn.hashdata.bireme.AbstractCommitCallback;
import cn.hashdata.bireme.ChangeSet;
import cn.hashdata.bireme.CommitCallback;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.BiremeException;
import cn.hashdata.bireme.Provider;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.RowSet;

/**
 * {@code KafkaProvider} is able to poll change data from Kafka.
 *
 * @author yuze
 *
 */
public abstract class KafkaProvider extends Provider {
  protected static final Long TIMEOUT_MS = 1000L;
  public static final String ABSTRACT_PROVIDER_TYPE = "Kafka";

  public static class KafkaProviderConfig extends ProviderConfig {
    public String topic;
    public String server;
    public String groupID;
  }

  public KafkaProviderConfig providerConfig;
  protected KafkaConsumer<String, String> consumer;
  private LinkedBlockingQueue<KafkaCommitCallback> commitCallbacks;

  /**
   * Create a new {@code kafkaProvider}.
   *
   * @param cxt the {@code Context}
   * @param providerConfig configuration for the {@code KafkaProvider}
   * @param test unitest or not
   */
  public KafkaProvider(Context cxt, KafkaProviderConfig providerConfig, boolean test) {
    super(cxt, providerConfig);
    this.providerConfig = providerConfig;
    this.commitCallbacks = new LinkedBlockingQueue<KafkaCommitCallback>();

    if (!test) {
      consumer = new KafkaConsumer<String, String>(kafkaProps());
      consumer.assign(createTopicPartitions());
    }
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
    props.put("group.id", providerConfig.groupID);
    props.put("enable.auto.commit", false);
    props.put("session.timeout.ms", 30000);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest");
    return props;
  }

  private ChangeSet packRecords(ConsumerRecords<String, String> records,
      KafkaCommitCallback callback) throws BiremeException {
    ChangeSet changeSet;

    try {
      changeSet = cxt.idleChangeSets.borrowObject();
      changeSet.provider = this;
      changeSet.createdAt = new Date();
      changeSet.changes = records;
      changeSet.callback = callback;
    } catch (Exception e) {
      String message = "Can't not borrow ChangeSet from the Object Pool.\n";
      throw new BiremeException(message, e);
    }

    return changeSet;
  }

  /**
   * Create a list of {@code TopicPartition} to subscribe according configuration.
   *
   * @return an ArrayList of {@code TopicPartition}
   */
  protected abstract ArrayList<TopicPartition> createTopicPartitions();

  /**
   * Start the {@code KafkaProvider}. It constantly poll data from the designated
   * {@code TopicPartition} and pack the change data into {@code ChangeSet}, transfer it to the
   * Change Set Queue in Context.
   *
   * @throws BiremeException can not borrow changeset from object pool
   * @throws InterruptedException if interrupted while waiting
   */
  @Override
  public Long call() throws BiremeException, InterruptedException {
    Thread.currentThread().setName("Provider " + getProviderName());

    logger.info("Provider {} Start.", getProviderName());

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
          throw new BiremeException(Message);
        }

        changeSet = packRecords(records, callback);

        recordMeter.mark(records.count());

        do {
          success = changeSetOut.offer(changeSet, TIMEOUT_MS, TimeUnit.MILLISECONDS);
          checkAndCommit();
        } while (!success && !cxt.stop);
      }
    } catch (BiremeException e) {
      logger.fatal("Provider {} exit on error. Message ", getProviderName(), e.getMessage());
      logger.fatal("Stack Trace: ", e);
      throw e;
    } finally {
      try {
        consumer.close();
      } catch (Exception ignore) {
      }
    }

    logger.info("Provider {} exit.", getProviderName());
    return 0L;
  }

  /**
   * Loop through the {@code ChangeSet} and transform each change data into a {@code Row}.
   *
   * @author yuze
   *
   */
  public abstract class KafkaTransformer extends Transformer {
    @SuppressWarnings("unchecked")
    @Override
    public void fillRowSet(RowSet rowSet) throws BiremeException {
      CommitCallback callback = changeSet.callback;
      HashMap<TopicPartition, Long> offsets = ((KafkaCommitCallback) callback).partitionOffset;
      Row row = null;

      for (ConsumerRecord<String, String> change :
          (ConsumerRecords<String, String>) changeSet.changes) {
        try {
          row = cxt.idleRows.borrowObject();
        } catch (Exception e) {
          String message = "Can't not borrow RowSet from the Object Pool.";
          throw new BiremeException(message, e);
        }

        if (!transform(change, row)) {
          cxt.idleRows.returnObject(row);
          continue;
        }

        row.receiveTime = changeSet.createdAt.getTime();
        addToRowSet(row, rowSet);
        offsets.put(new TopicPartition(change.topic(), change.partition()), change.offset());
      }

      callback.setNumOfTables(rowSet.rowBucket.size());
      rowSet.callback = callback;
    }

    /**
     * Transform the change data into a {@code Row}.
     *
     * @param change the change data
     * @param row an empty {@code Row} to store the result.
     * @return {@code true} if transform the change data successfully, {@code false} it the change
     *         data is null or filtered
     * @throws BiremeException when can not get the field
     */
    public abstract boolean transform(ConsumerRecord<String, String> change, Row row)
        throws BiremeException;
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
