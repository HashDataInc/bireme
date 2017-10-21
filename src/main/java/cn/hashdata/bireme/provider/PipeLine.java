package cn.hashdata.bireme.provider;

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
import cn.hashdata.bireme.Record;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.RowCache;
import cn.hashdata.bireme.RowSet;
import cn.hashdata.bireme.Table;

public abstract class PipeLine implements Callable<PipeLine>{
  public enum PipeLineState {
    NORMAL, ERROR, STOP
  }

  public Logger logger;
  
  public volatile PipeLineState state;
  public BiremeException e;

  public Context cxt;
  public SourceConfig conf;

  public LinkedBlockingQueue<Future<RowSet>> transResult;
  private LinkedList<Transformer> localTransformer;

  private Dispatcher dispatcher;

  public ConcurrentHashMap<String, RowCache> cache;

  public PipeLine(Context cxt, SourceConfig conf) {
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
  }

  @Override
  public PipeLine call() {
    // Poll data and start transformer TODO add comment
    while (transResult.remainingCapacity() != 0) {
      ChangeSet changeSet = null;
      try {
        changeSet = pollChangeSet();
      } catch (BiremeException e) {
        state = PipeLineState.ERROR;
        this.e = e;
        return this;
      }

      if (changeSet == null) {
        break;
      }

      Transformer trans = localTransformer.remove();
      trans.setChangeSet(changeSet);
      startTransform(trans);
      localTransformer.add(trans);
    }

    // Start dispatcher, only one dispatcher for each pipeline
    try {
      dispatcher.start();
    } catch (BiremeException e) {
      state = PipeLineState.ERROR;
      this.e = new BiremeException("Dispatch failed.\n", e.getCause());
      return this;
    }

    // Start merger
    for (RowCache rowCache : cache.values()) {
      if (rowCache.shouldMerge()) {
        try {
          rowCache.startMerge();
        } catch (BiremeException e) {
          state = PipeLineState.ERROR;
          this.e = e;
          return this;
        }
      }
      rowCache.startLoad();
    }

    // Commit result
    checkAndCommit();
    return this;
  }

  public abstract ChangeSet pollChangeSet() throws BiremeException;

  public abstract void checkAndCommit();

  public abstract Transformer createTransformer();

  private void startTransform(Transformer trans) {
    ExecutorService transformerPool = cxt.transformerPool;
    Future<RowSet> result = transformerPool.submit(trans);
    transResult.add(result);
  }

  /**
   * Get the unique name for the provider, which is specified in the configuration file.
   *
   * @return the name for the provider
   */
  public String getPipeLineName() {
    return conf.name;
  }

  /**
   * {@code Transformer} convert a group of change data to unified form.
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
      RowSet rowSet = null;

      try {
        rowSet = cxt.idleRowSets.borrowObject();
      } catch (Exception e) {
        String message = "Can't not borrow RowSet from the Object Pool.";
        throw new BiremeException(message, e);
      }

      fillRowSet(rowSet);

      cxt.idleChangeSets.returnObject(changeSet);
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
    protected String formatColumns(Record record, Table table, ArrayList<Integer> columns,
        boolean oldValue) throws BiremeException {
      tupleStringBuilder.setLength(0);

      for (int i = 0; i < columns.size(); ++i) {
        int columnIndex = columns.get(i);
        int sqlType = table.columnType.get(columnIndex);
        String columnName = table.columnName.get(columnIndex);
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
              int precision = table.columnPrecision.get(columnIndex);
              tupleStringBuilder.append(decodeToBit(data, precision));
              break;
            }

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP: {
              int scale = table.columnScale.get(columnIndex);
              String time = decodeToTime(data, sqlType, scale);
              tupleStringBuilder.append(time);
              break;
            }

            case Types.DECIMAL:
            case Types.NUMERIC: {
              int scale = table.columnScale.get(columnIndex);
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
            // TODO logger.warn("illegal character 0x00, deleted.");
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
     * @throws BiremeException Exceptions when add {@code Row} to {@code RowSet}
     */
    public void addToRowSet(Row row, RowSet rowSet) throws BiremeException {
      HashMap<String, ArrayList<Row>> bucket = rowSet.rowBucket;
      String mappedTable = row.mappedTable;
      ArrayList<Row> array = bucket.get(mappedTable);

      if (array == null) {
        try {
          array = cxt.idleRowArrays.borrowObject();
        } catch (Exception e) {
          String message = "Can't not borrow RowArray from the Object Pool.";
          throw new BiremeException(message, e);
        }

        bucket.put(mappedTable, array);
      }

      array.add(row);
    }
  }
}
