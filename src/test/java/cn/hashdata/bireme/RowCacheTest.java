package cn.hashdata.bireme;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import cn.hashdata.bireme.Config;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.RowCache;

public class RowCacheTest {
  @Mock Row row;

  Config conf;
  Context cxt;

  @Before
  public void setup() throws Exception {
    conf = TestUtil.generateConfig();
    TestUtil.addMaxellDataSource(conf, 2);
    cxt = new Context(conf, true);
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testRowCacheSizeLimit() {
    int cacheSize = conf.row_cache_size;

    // get a random RowCache
    RowCache cache = cxt.tableRowCache.values().iterator().next();

    for (int count = 0; count < cacheSize; count++) {
      assertTrue(cache.rows.offer(row));
    }

    assertFalse(cache.rows.offer(row));
  }
}
