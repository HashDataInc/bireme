package cn.hashdata.dbsync;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
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

import cn.hashdata.dbsync.provider.MaxwellChangeProvider;

import java.lang.reflect.Field;

public class MaxwellChangeProviderTest {
  @Mock Meter mockProviderMeter;

  @Mock KafkaConsumer<String, String> mockConsumer;

  @Mock Logger logger;

  @InjectMocks MaxwellChangeProvider provider;

  @Mock ConsumerRecords<String, String> mockRecord;

  @Mock ChangeSet mockChangeSet;

  Config conf;
  Context cxt;
  @Before
  public void setup() throws Exception {
    conf = TestUtil.generateConfig();
    TestUtil.addMaxellDataSource(conf, 2);
    cxt = new Context(conf, true);
    provider = new MaxwellChangeProvider(cxt, conf.maxwellConf.get(0), true);
    MockitoAnnotations.initMocks(this);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testChangeSetQueueBlocking() throws Exception {
    PowerMockito.when(mockConsumer.poll(Mockito.anyLong())).thenReturn(mockRecord);
    PowerMockito.when(mockRecord.isEmpty()).thenReturn(false);
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
