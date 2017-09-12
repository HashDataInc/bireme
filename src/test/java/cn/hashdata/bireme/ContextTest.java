package cn.hashdata.bireme;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import cn.hashdata.bireme.Config;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.RowCache;

public class ContextTest {
  Config conf;
  Context cxt;

  @Before
  public void setup() throws Exception {
    conf = TestUtil.generateConfig();
    TestUtil.addKafkaDataSource(conf, 2);
    cxt = new Context(conf, true);
  }

  @Test
  public void testCapacity() {
    assertTrue(cxt.changeSetQueue.remainingCapacity() == conf.changeset_queue_size);
    assertTrue(cxt.tableRowCache.size() == conf.loadersCount);

    Iterator<RowCache> iterator = cxt.tableRowCache.values().iterator();
    while (iterator.hasNext()) {
      RowCache cache = iterator.next();
      assertTrue(cache.rows.remainingCapacity() == conf.batch_size * 2);
      assertTrue(
          cache.rowBatchMergers.remainingCapacity() == conf.row_cache_size / conf.batch_size);
    }
  }

  @Test
  public void testRowBorrowAndReturn() throws Exception {
    Row t;
    ArrayList<Row> array = new ArrayList<Row>();
    Random random = new Random();
    int active = 0;
    int idle = 0;

    for (int count = 0; count < 10000; count++) {
      if (random.nextInt(2) == 0) {
        t = cxt.idleRows.borrowObject();
        array.add(t);

        if (idle != 0) {
          idle--;
        }
        active++;
      } else if (!array.isEmpty()) {
        int index = random.nextInt(array.size());
        cxt.idleRows.returnObject(array.remove(index));

        active--;
        idle++;
      }
    }

    assertTrue(cxt.idleRows.getNumIdle() == idle);
    assertTrue(cxt.idleRows.getNumActive() == active);
  }
}
