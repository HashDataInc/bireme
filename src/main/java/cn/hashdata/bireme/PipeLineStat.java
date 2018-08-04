package cn.hashdata.bireme;

import java.util.Date;
import java.util.HashMap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import cn.hashdata.bireme.pipeline.PipeLine;

/**
 * {@code PipeLineStat} contains the statistics about the {@code PipeLine}.
 *
 * @author yuze
 *
 */
public class PipeLineStat {
  private static final Long SCECOND_TO_MILLISECOND = 1000L;
  private String pipeLineName;
  public Long newestCompleted = 0L;
  public Long delay = 0L;

  private PipeLine pipeLine;
  private MetricRegistry register;

  public Timer avgDelay;
  public Meter recordCount;
  public Gauge<String> pipeLineState;
  public Gauge<String> completed;
  public Gauge<String> syncGap;
  public Gauge<Long> transformQueueSize;

  public HashMap<String, Gauge<Long>> cacheSize;
  public HashMap<String, Timer> copyForDelete;
  public HashMap<String, Timer> delete;
  public HashMap<String, Timer> insert;

  public PipeLineStat(PipeLine pipeLine) {
    this.pipeLine = pipeLine;
    this.pipeLineName = pipeLine.myName;
    this.register = pipeLine.cxt.register;

    cacheSize = new HashMap<String, Gauge<Long>>();
    copyForDelete = new HashMap<String, Timer>();
    delete = new HashMap<String, Timer>();
    insert = new HashMap<String, Timer>();

    avgDelay = register.timer(MetricRegistry.name(pipeLineName, "AvgDelay"));
    recordCount = register.meter(MetricRegistry.name(pipeLineName, "RecordCount"));

    pipeLineState = new Gauge<String>() {
      @Override
      public String getValue() {
        return pipeLine.state.toString();
      }
    };

    completed = new Gauge<String>() {
      @Override
      public String getValue() {
        return new Date(newestCompleted * SCECOND_TO_MILLISECOND).toString();
      }
    };

    syncGap = new Gauge<String>() {
      @Override
      public String getValue() {
        Long now = new Date().getTime() / SCECOND_TO_MILLISECOND;
        return String.valueOf(now - newestCompleted) + "seconds.";
      }
    };

    transformQueueSize = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return (long) pipeLine.transResult.size();
      }
    };

    register.register(MetricRegistry.name(pipeLineName, "PipeLine State"), pipeLineState);
    register.register(MetricRegistry.name(pipeLineName, "Completed"), completed);
    register.register(MetricRegistry.name(pipeLineName, "SyncGap"), syncGap);
    register.register(MetricRegistry.name(pipeLineName, "TransformQueueSize"), transformQueueSize);
  }

  /**
   * Add {@code Gauge} for a {@link RowCache} to get the size.
   *
   * @param table the name of the cache
   * @param cache the {@code RowCache} itselff
   */
  public void addGaugeForCache(String table, RowCache cache) {
    Gauge<Long> gauge = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return (long) cache.rows.size();
      }
    };

    cacheSize.put(table, gauge);
    register.register(MetricRegistry.name(pipeLineName, table, "Cache"), gauge);
  }

  /**
   * Add {@code Timer}s for a {@link ChangeLoader}. Each {@code ChangeLoader} has three Timer.
   * <ul>
   * <li>Record the time of copy followed by delete</li>
   * <li>Record the time of delete</li>
   * <li>Record the time of insert in copy way</li>
   * </ul>
   *
   * @param table the {@code ChangeLoader}'s table
   * @return the registered Timer
   */
  public Timer[] addTimerForLoader(String table) {
    Timer[] timers = new Timer[3];
    timers[0] = register.timer(MetricRegistry.name(pipeLineName, table, "Loader", "CopyForDelete"));
    timers[1] = register.timer(MetricRegistry.name(pipeLineName, table, "Loader", "Delete"));
    timers[2] = register.timer(MetricRegistry.name(pipeLineName, table, "Loader", "Insert"));

    copyForDelete.put(table, timers[0]);
    delete.put(table, timers[1]);
    insert.put(table, timers[2]);
    return timers;
  }
}
