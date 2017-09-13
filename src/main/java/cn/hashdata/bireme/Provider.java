/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import cn.hashdata.bireme.provider.ProviderConfig;

/**
 * {@code Provider} is responsible for polling data from data source and provide to
 * {@code Dispatcher}. Each {@code Provider} must its own {@code Transformer}, which could transform
 * the polled data to bireme inner format.
 *
 * @author yuze
 *
 */
public abstract class Provider implements Callable<Long> {
  protected static final Long TIMEOUT_MS = 1000L;

  protected Logger logger;
  protected Meter recordMeter;

  protected Context cxt;
  protected ProviderConfig conf;
  protected HashMap<String, String> tableMap;
  protected LinkedBlockingQueue<ChangeSet> changeSetOut;
  protected LinkedBlockingQueue<Transformer> idleTransformer;

  public Provider(Context cxt, ProviderConfig conf) {
    this.cxt = cxt;
    this.conf = conf;
    this.tableMap = cxt.tableMap;
    this.changeSetOut = cxt.changeSetQueue;
    this.idleTransformer = new LinkedBlockingQueue<Transformer>();

    this.logger = LogManager.getLogger("Bireme." + Provider.class + " " + getProviderName());
    this.recordMeter = cxt.metrics.meter(MetricRegistry.name(Provider.class, getProviderName()));
  }

  /**
   * Get the type of the provider.
   *
   * @return the type of the provider
   */
  public String getProviderType() {
    return conf.type.toString();
  }

  /**
   * Get the unique name for the provider, which is specified in the configuration file.
   *
   * @return the name for the provider
   */
  public String getProviderName() {
    return conf.name;
  }

  /**
   * Create a new transformer corresponding to the provider.
   *
   * @return the new created transformer
   */
  abstract public Transformer createTransformer();

  /**
   * Borrow a {@code Transformer} from the {@code Provider} and set the {@code ChangeSet} to be
   * transformer. This method should be non-blocking. If no {@code Transformer} is available
   * currently, create a new {@code Transformer}.
   *
   * @param changeSet The {@code ChangeSet} that need to be transformed.
   * @return The borrowed {@code Transformer}.
   */
  public Transformer borrowTransformer(ChangeSet changeSet) {
    Transformer transformer = idleTransformer.poll();

    if (transformer == null) {
      transformer = createTransformer();
    }

    transformer.setChangeSet(changeSet);

    return transformer;
  };

  /**
   * Return the borrowed {@code Transformer} to the pool.
   *
   * @param trans The {@code Transformer} should be returned.
   */
  public void returnTransformer(Transformer trans) {
    trans.setChangeSet(null);
    idleTransformer.offer(trans);
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
     */
    @Override
    public RowSet call() throws BiremeException {
      RowSet rowSet = null;

      try {
        rowSet = cxt.idleRowSets.borrowObject();
        rowSet.createdAt = changeSet.createdAt;
      } catch (Exception e) {
        String message = "Can't not borrow RowSet from the Object Pool.";
        throw new BiremeException(message, e);
      }

      fillRowSet(rowSet);

      cxt.idleChangeSets.returnObject(changeSet);

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
     */
    protected String formatColumns(
        Record record, Table table, ArrayList<Integer> columns, boolean oldValue) {
      tupleStringBuilder.setLength(0);

      for (int i = 0; i < columns.size(); ++i) {
        int columnIndex = columns.get(i);
        String columnName = table.columnName.get(columnIndex);
        String data = record.getField(columnName, oldValue);

        switch (table.columnType.get(columnIndex)) {
          case Types.CHAR:
          case Types.NCHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.NVARCHAR:
          case Types.LONGNVARCHAR: {
            if (data != null) {
              tupleStringBuilder.append(QUOTE);
              tupleStringBuilder.append(escapeString(data));
              tupleStringBuilder.append(QUOTE);
            }
            break;
          }

          case Types.BINARY:
          case Types.BLOB:
          case Types.CLOB:
          case Types.LONGVARBINARY:
          case Types.NCLOB:
          case Types.VARBINARY: {
            if (data != null) {
              byte[] decoded = null;
              decoded = decodeToBinary(data);
              tupleStringBuilder.append(escapeBinary(decoded));
            }
            break;
          }

          case Types.BIT: {
            if (data != null) {
              int precision = table.columnPrecision.get(columnIndex);
              tupleStringBuilder.append(decodeToBit(data, precision));
            }
            break;
          }

          default: {
            if (data != null) {
              tupleStringBuilder.append(data);
            }
            break;
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
     * For binary type, {@code Transformer} need to decode the extracted string and decode it to
     * origin binary.
     *
     * @param data the encoded string
     * @return the array of byte, decode result
     */
    protected abstract byte[] decodeToBinary(String data);

    /**
     * For bit type, {@code Transformer} need to decode the extracted string and decode it to
     * origin bit.
     *
     * @param data the encoded string
     * @param precision the length of the bit field, acquired from the table's metadata
     * @return the  string of 1 or 0
     */

    protected abstract String decodeToBit(String data, int precision);

    /**
     * Add escape character to a data string.
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
     * Get the outer {@code Provider} of this {@code Transformer}.
     *
     * @return the outer {@code Provider}
     */
    public Provider getProvider() {
      return Provider.this;
    }

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
