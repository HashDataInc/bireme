package cn.hashdata.bireme.pipeline;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.Logger;

import cn.hashdata.bireme.BiremeException;
import cn.hashdata.bireme.ChangeSet;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.Dispatcher;
import cn.hashdata.bireme.PipeLineStat;
import cn.hashdata.bireme.Record;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.RowCache;
import cn.hashdata.bireme.RowSet;
import cn.hashdata.bireme.Table;

/**
 * {@code PipeLine} is a bridge between data source and target table. The data flow order is
 * guaranteed. A {@code PipeLine} does four things as follows:
 * <ul>
 * <li>Poll data and allocate {@link Transformer} to convert the data.</li>
 * <li>Dispatch the transformed data and insert it into {@link RowCache}.</li>
 * <li>Drive the {@code RowBatchMerger} to work</li>
 * <li>Drivet the {@code ChangeLoader} to work</li>
 * </ul>
 *
 * @author yuze
 *
 */
public abstract class PipeLine implements Callable<PipeLine> {
  public enum PipeLineState { NORMAL, ERROR }

  public Logger logger;

  public String myName;
  public volatile PipeLineState state;
  public Exception e;
  public PipeLineStat stat;

  public Context cxt;
  public SourceConfig conf;

  public LinkedBlockingQueue<Future<RowSet>> transResult;
  private LinkedList<Transformer> localTransformer;

  private Dispatcher dispatcher;

  public ConcurrentHashMap<String, RowCache> cache;

  public PipeLine(Context cxt, SourceConfig conf, String myName) {
    this.myName = myName;
    this.state = PipeLineState.NORMAL;
    this.e = null;

    this.cxt = cxt;
    this.conf = conf;

    int queueSize = cxt.conf.transform_queue_size;

    transResult = new LinkedBlockingQueue<Future<RowSet>>(queueSize);
    localTransformer = new LinkedList<Transformer>();

    cache = new ConcurrentHashMap<String, RowCache>();

    dispatcher = new Dispatcher(cxt, this);

    for (int i = 0; i < queueSize; i++) {
      localTransformer.add(createTransformer());
    }

    // initialize statistics
    this.stat = new PipeLineStat(this);
  }

  @Override
  public PipeLine call() {
    try {
      executePipeline();
    } catch (Exception e) {
      state = PipeLineState.ERROR;
      this.e = e;

      logger.error("Execute Pipeline failed: {}", e.getMessage());
      logger.error("Stack Trace: ", e);
    }

    return this;
  }

  private PipeLine executePipeline() {
    // Poll data and start transformer
    if (transData() == false) {
      return this;
    }

    // Start dispatcher, only one dispatcher for each pipeline
    if (startDispatch() == false) {
      return this;
    }

    // Start merger
    if (startMerge() == false) {
      return this;
    }

    checkAndCommit(); // Commit result
    return this;
  }

  private boolean transData() {
    while (transResult.remainingCapacity() != 0) {
      ChangeSet changeSet = null;

      try {
        changeSet = pollChangeSet();
      } catch (BiremeException e) {
        state = PipeLineState.ERROR;
        this.e = e;

        logger.error("Poll change set failed. Message: {}", e.getMessage());
        logger.error("Stack Trace: ", e);

        return false;
      }

      if (changeSet == null) {
        break;
      }

      Transformer trans = localTransformer.remove();
      trans.setChangeSet(changeSet);
      startTransform(trans);
      localTransformer.add(trans);
    }

    return true;
  }

  private boolean startDispatch() {
    try {
      dispatcher.dispatch();
    } catch (BiremeException e) {
      state = PipeLineState.ERROR;
      this.e = e;

      logger.error("Dispatch failed. Message: {}", e.getMessage());
      logger.error("Stack Trace: ", e);

      return false;

    } catch (InterruptedException e) {
      state = PipeLineState.ERROR;
      this.e = new BiremeException("Dispatcher failed, be interrupted", e);

      logger.info("Interrupted when getting transform result. Message: {}.", e.getMessage());
      logger.info("Stack Trace: ", e);

      return false;
    }

    return true;
  }

  private boolean startMerge() {
    for (RowCache rowCache : cache.values()) {
      if (rowCache.shouldMerge()) {
        rowCache.startMerge();
      }

      try {
        rowCache.startLoad();

      } catch (BiremeException e) {
        state = PipeLineState.ERROR;
        this.e = e;

        logger.info("Loader for {} failed. Message: {}.", rowCache.tableName, e.getMessage());
        logger.info("Stack Trace: ", e);

        return false;

      } catch (InterruptedException e) {
        state = PipeLineState.ERROR;
        this.e = new BiremeException("Get Future<Long> failed, be interrupted", e);

        logger.info("Interrupted when getting loader result for {}. Message: {}.",
            rowCache.tableName, e.getMessage());
        logger.info("Stack Trace: ", e);

        return false;
      }
    }

    return true;
  }

  /**
   * Poll a set of change data from source and pack it to {@link ChangeSet}.
   *
   * @return a packed change set
   * @throws BiremeException Exceptions when poll data from source
   */
  public abstract ChangeSet pollChangeSet() throws BiremeException;

  /**
   * Check whether the loading operation is complete. If true, commit it.
   *
   */
  public abstract void checkAndCommit();

  /**
   * Create a new {@link Transformer} to work parallel.
   *
   * @return a new {@code Transformer}
   */
  public abstract Transformer createTransformer();

  private void startTransform(Transformer trans) {
    ExecutorService transformerPool = cxt.transformerPool;
    Future<RowSet> result = transformerPool.submit(trans);
    transResult.add(result);
  }

  /**
   * Get the unique name for the {@code PipeLine}.
   *
   * @return the name for the {@code PipeLine}
   */
  public String getPipeLineName() {
    return conf.name;
  }

  /**
   * {@code Transformer} convert a group of change data to unified form {@link Row}.
   *
   * @author yuze
   *
   */
  public abstract class Transformer implements Callable<RowSet> {
    private static final char FIELD_DELIMITER = '|';
    private static final char NEWLINE = '\n';
    private static final char QUOTE = '"';
    private static final char ESCAPE = '\\';

    public ChangeSet changeSet;
    public StringBuilder tupleStringBuilder;
    public StringBuilder fieldStringBuilder;

    public Transformer() {
      tupleStringBuilder = new StringBuilder();
      fieldStringBuilder = new StringBuilder();
    }

    /**
     * Borrow an empty {@code RowSet} and write the data acquired from {@code ChangeSet} to the
     * {@code RowSet}. Finally, return the filled {@code RowSet}.
     *
     * @throws BiremeException when unable to transform the recoed
     */
    @Override
    public RowSet call() throws BiremeException {
      RowSet rowSet = new RowSet();

      fillRowSet(rowSet);

      changeSet.destory();
      changeSet = null;

      return rowSet;
    }

    /**
     * Format the change data into csv tuple, which is then loaded to database by COPY.
     *
     * @param record contain change data polled by {@code Provider}.
     * @param table metadata of the target table
     * @param columns the indexes of columns to assemble a csv tuple
     * @param oldValue only for update operation when primary key was updated, we need to get the
     *        old key and delete the old tuple
     * @return the csv tuple in string
     * @throws BiremeException when can not get the field value
     */
    protected String formatColumns(Record record, Table table, ArrayList<String> columns,
        boolean oldValue) throws BiremeException {
      tupleStringBuilder.setLength(0);

      for (int i = 0; i < columns.size(); ++i) {
        String columnName = columns.get(i);
        int sqlType = table.columnType.get(columnName);
        String data = null;

        data = record.getField(columnName, oldValue);
        if (data != null) {
          switch (sqlType) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: {
              tupleStringBuilder.append(QUOTE);
              tupleStringBuilder.append(escapeString(data));
              tupleStringBuilder.append(QUOTE);

              break;
            }

            case Types.BINARY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.LONGVARBINARY:
            case Types.NCLOB:
            case Types.VARBINARY: {
              byte[] decoded = null;
              decoded = decodeToBinary(data);
              tupleStringBuilder.append(escapeBinary(decoded));
              break;
            }

            case Types.BIT: {
              int precision = table.columnPrecision.get(columnName);
              tupleStringBuilder.append(decodeToBit(data, precision));
              break;
            }

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP: {
              int scale = table.columnScale.get(columnName);
              String time = decodeToTime(data, sqlType, scale);
              tupleStringBuilder.append(time);
              break;
            }

            case Types.DECIMAL:
            case Types.NUMERIC: {
              int scale = table.columnScale.get(columnName);
              String numeric = decodeToNumeric(data, sqlType, scale);
              tupleStringBuilder.append(numeric);
              break;
            }

            default: {
              tupleStringBuilder.append(data);
              break;
            }
          }
        }
        if (i + 1 < columns.size()) {
          tupleStringBuilder.append(FIELD_DELIMITER);
        }
      }
      tupleStringBuilder.append(NEWLINE);

      return tupleStringBuilder.toString();
    }

    /**
     * For binary type, {@code Transformer} need to decode the extracted string and transform it to
     * origin binary.
     *
     * @param data the encoded string
     * @return the array of byte, decode result
     */
    protected abstract byte[] decodeToBinary(String data);

    /**
     * For bit type, {@code Transformer} need to decode the extracted string and transform it to
     * origin bit.
     *
     * @param data the encoded string
     * @param precision the length of the bit field, acquired from the table's metadata
     * @return the string of 1 or 0
     */
    protected abstract String decodeToBit(String data, int precision);

    /**
     * For Date/Time type, {@code Transformer} need to decode the extracted string and transform it
     * to origin Date/Time string.
     *
     * @param data the encoded string from provider
     * @param sqlType particular type of this field, such as Time, Date
     * @param precision specifies the number of fractional digits retained in the seconds field
     * @return the Date/Time format
     */
    protected String decodeToTime(String data, int sqlType, int precision) {
      return data;
    };

    /**
     * For Numeric type, {@code Transformer} need to decode the extracted string and transform it to
     * origin Numeric in String.
     *
     * @param data the value from provider
     * @param sqlType particular type of this field
     * @param precision the count of decimal digits in the fractional part
     * @return the numeric number in String
     */
    protected String decodeToNumeric(String data, int sqlType, int precision) {
      return data;
    };

    /**
     * Add escape character to a data string.
     *
     * @param data the origin string
     * @return the modified string
     */
    protected String escapeString(String data) {
      fieldStringBuilder.setLength(0);

      for (int i = 0; i < data.length(); ++i) {
        char c = data.charAt(i);

        switch (c) {
          case 0x00:
            logger.warn("illegal character 0x00, deleted.");
            continue;
          case QUOTE:
          case ESCAPE:
            fieldStringBuilder.append(ESCAPE);
        }

        fieldStringBuilder.append(c);
      }

      return fieldStringBuilder.toString();
    }

    /**
     * Encode the binary data into string for COPY into target database.
     *
     * @param data the origin binary data
     * @return the encoded string
     */
    protected String escapeBinary(byte[] data) {
      fieldStringBuilder.setLength(0);

      for (int i = 0; i < data.length; ++i) {
        if (data[i] == '\\') {
          fieldStringBuilder.append('\\');
          fieldStringBuilder.append('\\');
        } else if (data[i] < 0x20 || data[i] > 0x7e) {
          byte b = data[i];
          char[] val = new char[3];
          val[2] = (char) ((b & 07) + '0');
          b >>= 3;
          val[1] = (char) ((b & 07) + '0');
          b >>= 3;
          val[0] = (char) ((b & 03) + '0');
          fieldStringBuilder.append('\\');
          fieldStringBuilder.append(val);
        } else {
          fieldStringBuilder.append((char) (data[i]));
        }
      }

      return fieldStringBuilder.toString();
    }

    /**
     * Appoint a {@code ChangeSet} to the {@code Transformer}
     *
     * @param changeSet a package of change data
     */
    public void setChangeSet(ChangeSet changeSet) {
      this.changeSet = changeSet;
    }

    /**
     * Write the change data into a {@code RowSet}.
     *
     * @param rowSet a empty {@code RowSet} to store change data
     * @throws BiremeException Exceptions when fill the {@code RowSet}
     */
    public abstract void fillRowSet(RowSet rowSet) throws BiremeException;

    /**
     * After convert a single change data to a {@code Row}, insert into the {@code RowSet}.
     *
     * @param row the converted change data
     * @param rowSet the {@code RowSet} to organize the {@code Row}
     */
    public void addToRowSet(Row row, RowSet rowSet) {
      HashMap<String, ArrayList<Row>> bucket = rowSet.rowBucket;
      String mappedTable = row.mappedTable;
      ArrayList<Row> array = bucket.get(mappedTable);

      if (array == null) {
        array = new ArrayList<Row>();
        bucket.put(mappedTable, array);
      }

      array.add(row);
    }
  }
}
