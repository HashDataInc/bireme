package cn.hashdata.bireme;

import java.util.Date;
import java.util.HashMap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import cn.hashdata.bireme.provider.PipeLine;

/**
 * {@code LoadState} document current state for {@code ChangeLoader}. It is in connection with
 * {@code LoadTask}. Each {@code LoadState} has three kinds of times.
 * <ul>
 * <li><B> produceTime </B> The time when Change Data Capture (CDC) produced the record. For each
 * origin table, LoadState documents the latest produceTime in the corresponding LoadTask.</li>
 * <li><B> receiveTime </B> The time when Bireme received the record. LoadState documents the
 * earliest receiveTime for each origin table in the corresponding LoadTask.</li>
 * <li><B> completeTime </B> The time when ChangeLoader successfully loaded the LoadTask.</li>
 * </ul>
 *
 * @author yuze
 *
 */
public class PipeLineStat {
  private String pipeLineName;
  public Long newestCompleted = 0L;

  private PipeLine pipeLine;
  private MetricRegistry register;

  public Timer avgDelay;
  public Meter recordCount;
  public Gauge<String> completed;
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

    completed = new Gauge<String>() {

      @Override
      public String getValue() {
        return new Date(newestCompleted).toString();
      }
    };

    transformQueueSize = new Gauge<Long>() {

      @Override
      public Long getValue() {
        return (long) pipeLine.transResult.size();
      }

    };

    register.register(MetricRegistry.name(pipeLineName, "Completed"), completed);
    register.register(MetricRegistry.name(pipeLineName, "TransformQueueSize"), transformQueueSize);
  }

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
