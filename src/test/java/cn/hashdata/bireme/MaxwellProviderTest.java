package cn.hashdata.bireme;

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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.powermock.api.mockito.PowerMockito;

import com.codahale.metrics.Meter;

import cn.hashdata.bireme.ChangeSet;
import cn.hashdata.bireme.Config;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.Table;
import cn.hashdata.bireme.Provider.Transformer;
import cn.hashdata.bireme.provider.KafkaProvider.KafkaProviderConfig;
import cn.hashdata.bireme.provider.MaxwellProvider;

public class MaxwellProviderTest {
  @Mock Meter mockProviderMeter;
  @Mock KafkaConsumer<String, String> mockConsumer;
  @Mock Logger logger;
  @InjectMocks Provider provider;

  @InjectMocks Context cxt;

  @Mock ChangeSet mockChangeSet;
  @Mock ConsumerRecords<String, String> mockRecords;

  @Rule public final ExpectedException exception = ExpectedException.none();

  Config conf;
  KafkaProviderConfig kafkaConf;
  Table table;

  @Before
  public void setup() throws Exception {
    conf = TestUtil.generateConfig();
    TestUtil.addKafkaDataSource(conf, 1);
    kafkaConf = conf.maxwellConf.get(0);

    cxt = new Context(conf, true);
    provider = new MaxwellProvider(cxt, kafkaConf, true);
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testChangeSetQueueBlocking() throws Exception {
    PowerMockito.when(mockConsumer.poll(Mockito.anyLong())).thenReturn(mockRecords);
    PowerMockito.when(mockRecords.isEmpty()).thenReturn(false);

    LinkedBlockingQueue<ChangeSet> queue = cxt.changeSetQueue;

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

    LinkedBlockingQueue<Transformer> queue = provider.idleTransformer;

    assertTrue(queue.size() == idle);
    assertTrue(array.size() == active);
  }
}
