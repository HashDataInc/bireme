package cn.hashdata.dbsync;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class ContextTest {
  Config conf;
  Context cxt;

  @Before
  public void setup() throws Exception {
    conf = TestUtil.generateConfig();
    TestUtil.addMaxellDataSource(conf, 2);
    cxt = new Context(conf, true);
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
