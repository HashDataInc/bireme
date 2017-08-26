package cn.hashdata.dbsync;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.powermock.api.mockito.PowerMockito;

import com.codahale.metrics.Meter;
import com.google.gson.Gson;

import cn.hashdata.dbsync.Row.RowType;
import cn.hashdata.dbsync.provider.MaxwellChangeProvider;
import cn.hashdata.dbsync.provider.MaxwellChangeProvider.MaxwellChangeTransformer;
import cn.hashdata.dbsync.provider.MaxwellChangeProvider.MaxwellChangeTransformer.Record;

import java.lang.reflect.Field;
import java.sql.Types;

public class MaxwellChangeProviderTest {
  @Mock Meter mockProviderMeter;
  @Mock KafkaConsumer<String, String> mockConsumer;
  @Mock Logger logger;
  @InjectMocks MaxwellChangeProvider provider;

  @Mock(name = "tablesInfo") HashMap<String, Table> mockTableInfo;
  @InjectMocks Context cxt;

  @Mock ChangeSet mockChangeSet;
  @Mock ConsumerRecords<String, String> mockRecords;

  @Rule public final ExpectedException exception = ExpectedException.none();

  Config conf;
  Gson gson;
  Table table;

  @Before
  public void setup() throws Exception {
    conf = TestUtil.generateConfig();
    TestUtil.addMaxellDataSource(conf, 1);
    cxt = new Context(conf, true);
    gson = new Gson();
    provider = new MaxwellChangeProvider(cxt, conf.maxwellConf.get(0), true);
    MockitoAnnotations.initMocks(this);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testChangeSetQueueBlocking() throws Exception {
    PowerMockito.when(mockConsumer.poll(Mockito.anyLong())).thenReturn(mockRecords);
    PowerMockito.when(mockRecords.isEmpty()).thenReturn(false);
    Field f = provider.getClass().getDeclaredField("changeSetOut");
    f.setAccessible(true);
    LinkedBlockingQueue<ChangeSet> queue = (LinkedBlockingQueue<ChangeSet>) f.get(provider);

    assertTrue(queue.remainingCapacity() == conf.changeset_queue_size);

    cxt.stop = false;
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(provider);

    do {
      Thread.sleep(1);
    } while (queue.size() < conf.changeset_queue_size);

    assertFalse(queue.offer(mockChangeSet));
    Mockito.verify(mockConsumer, Mockito.times(conf.changeset_queue_size + 1))
        .poll(Mockito.anyLong());

    cxt.stop = true;
    executor.shutdown();
  }

  @Test
  public void testTransformRecord() throws DbsyncException, Exception {
    MaxwellChangeTransformer transformer =
        (MaxwellChangeTransformer) provider.borrowTransformer(mockChangeSet);
    String change =
        "{\"database\":\"demo\",\"table\":\"test\",\"type\":\"update\",\"ts\":1503728889,\"xid\":5024,\"commit\":true,\"data\":{\"column0\":1, \"column1\":\"dbsync\"},\"old\":{\"column0\":2}}";
    Record record = gson.fromJson(change, Record.class);

    ArrayList<Integer> types = new ArrayList<Integer>();
    types.add(Types.INTEGER);
    types.add(Types.VARCHAR);
    ArrayList<Integer> keyIndexs = new ArrayList<Integer>();
    keyIndexs.add(0);
    table = TestUtil.generateTableInfo(types, keyIndexs);

    PowerMockito.when(mockTableInfo.get(Mockito.any())).thenReturn(table);

    Row row = transformer.convertRecord(record, RowType.UPDATE);
    assertTrue(row.keys.equals("1\n"));
    assertTrue(row.oldKeys.equals("2\n"));
    assertTrue(row.tuple.equals("1|\"dbsync\"\n"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTransformerBorrowAndReturn() throws Exception {
    Transformer t;
    ArrayList<Transformer> array = new ArrayList<Transformer>();
    Random random = new Random();
    int active = 0;
    int idle = 0;

    for (int count = 0; count < 10000; count++) {
      if (random.nextInt(2) == 0) {
        t = provider.borrowTransformer(mockChangeSet);
        array.add(t);

        if (idle != 0) {
          idle--;
        }
        active++;
      } else if (!array.isEmpty()) {
        int index = random.nextInt(array.size());
        provider.returnTransformer(array.remove(index));

        active--;
        idle++;
      }
    }

    Field f = provider.getClass().getDeclaredField("idleTransformer");
    f.setAccessible(true);
    LinkedBlockingQueue<Transformer> queue = (LinkedBlockingQueue<Transformer>) f.get(provider);

    assertTrue(queue.size() == idle);
    assertTrue(array.size() == active);
  }
}
