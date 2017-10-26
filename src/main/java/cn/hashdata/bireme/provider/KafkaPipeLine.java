package cn.hashdata.bireme.provider;

import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.codahale.metrics.Timer;

import cn.hashdata.bireme.AbstractCommitCallback;
import cn.hashdata.bireme.BiremeException;
import cn.hashdata.bireme.ChangeSet;
import cn.hashdata.bireme.CommitCallback;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.RowSet;

public abstract class KafkaPipeLine extends PipeLine {
  protected KafkaConsumer<String, String> consumer;
  protected LinkedBlockingQueue<KafkaCommitCallback> commitCallbacks;

  public KafkaPipeLine(Context cxt, SourceConfig conf, String myName) {
    super(cxt, conf, myName);
    consumer = KafkaPipeLine.createConsumer(conf.server, conf.groupID);
    commitCallbacks = new LinkedBlockingQueue<KafkaCommitCallback>();
  }

  @Override
  public ChangeSet pollChangeSet() throws BiremeException {
    // TODO can set parameter as 0L?
    ConsumerRecords<String, String> records = consumer.poll(1000L);

    if (cxt.stop || records.isEmpty()) {
      return null;
    }

    KafkaCommitCallback callback = new KafkaCommitCallback();

    if (!commitCallbacks.offer(callback)) {
      String Message = "Can't add CommitCallback to queue.";
      throw new BiremeException(Message);
    }

    stat.recordCount.mark(records.count());

    return packRecords(records, callback);
  }

  @Override
  public void checkAndCommit() {
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

  private ChangeSet packRecords(ConsumerRecords<String, String> records,
      KafkaCommitCallback callback) throws BiremeException {
    ChangeSet changeSet;

    try {
      changeSet = cxt.idleChangeSets.borrowObject();
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
      HashMap<String, Long> offsets = ((KafkaCommitCallback) callback).partitionOffset;
      Row row = null;

      for (ConsumerRecord<String, String> change : (ConsumerRecords<String, String>) changeSet.changes) {
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

        addToRowSet(row, rowSet);
        offsets.put(change.topic() + "+" + change.partition(), change.offset());
        callback.setNewestRecord(row.produceTime);
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
    public HashMap<String, Long> partitionOffset;
    private Timer.Context timerCTX;
    private Date start;

    public KafkaCommitCallback() {
      this.partitionOffset = new HashMap<String, Long>();

      // record the time being created
      timerCTX = stat.avgDelay.time();
      start = new Date();
    }

    @Override
    public void commit() {
      HashMap<TopicPartition, OffsetAndMetadata> offsets =
          new HashMap<TopicPartition, OffsetAndMetadata>();
      partitionOffset.forEach((key, value) -> {
        String topic = key.split("\\+")[0];
        int partition = Integer.valueOf(key.split("\\+")[1]);

        offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(value + 1));
      });

      consumer.commitSync(offsets);
      committed.set(true);
      partitionOffset.clear();

      // record the time being committed
      timerCTX.stop();

      stat.newestCompleted = newestRecord;
      stat.delay = new Date().getTime() - start.getTime();
    }
  }

  public static KafkaConsumer<String, String> createConsumer(String server, String groupID) {
    Properties props = new Properties();
    props.put("bootstrap.servers", server);
    props.put("group.id", groupID);
    props.put("enable.auto.commit", false);
    props.put("session.timeout.ms", 30000);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest");
    return new KafkaConsumer<String, String>(props);
  }
}
