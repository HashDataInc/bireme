package cn.hashdata.dbsync;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import cn.hashdata.dbsync.provider.Record;

/**
 * {@code Provider} is responsible for polling data from data source and provide to
 * {@code Dispatcher}. Each {@code Provider} must have a unique name and maintain a pool of
 * {@code Transformer}, which could transform the polled data to dbsync inner format.
 *
 * @author yuze
 *
 */
public abstract class Provider implements Callable<Long> {
  protected static final Long TIMEOUT_MS = 1000L;

  protected Context cxt;
  protected HashMap<String, String> tableMap;
  protected LinkedBlockingQueue<ChangeSet> changeSetOut;
  private LinkedBlockingQueue<Transformer> idleTransformer;

  public Provider(Context cxt) {
    this.cxt = cxt;
    this.tableMap = cxt.tableMap;
    this.changeSetOut = cxt.changeSetQueue;
    this.idleTransformer = new LinkedBlockingQueue<Transformer>();
  }

  abstract public String getProviderType();

  abstract public String getProviderName();

  abstract public Transformer createTransformer();

  /**
   * Borrow a {@code Transformer} from the maintained pool. This method should be non-blocking. If
   * no {@code Transformer} is available currently, create a new {@code Transformer}.
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

    @Override
    public RowSet call() throws DbsyncException {
      RowSet rowSet = null;

      try {
        rowSet = cxt.idleRowSets.borrowObject();
        rowSet.createdAt = changeSet.createdAt;
      } catch (Exception e) {
        String message = "Can't not borrow RowSet from the Object Pool.";
        throw new DbsyncException(message, e);
      }
      fillRowSet(rowSet);

      cxt.idleChangeSets.returnObject(changeSet);

      return rowSet;
    }

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
              decoded = decodeString(data);
              tupleStringBuilder.append(escapeBinary(decoded));
            }
            break;
          }

          case Types.BIT: {
            if (data != null) {
              int precision = table.columnPrecision.get(columnIndex);
              String binaryStr = Integer.toBinaryString(Integer.valueOf(data));
              tupleStringBuilder.append(
                  String.format("%" + precision + "s", binaryStr).replace(' ', '0'));
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

    protected abstract byte[] decodeString(String data);

    protected String escapeString(String data) {
      fieldStringBuilder.setLength(0);

      for (int i = 0; i < data.length(); ++i) {
        char c = data.charAt(i);

        switch (c) {
          case 0x00:
            // logger.warn("illegal character 0x00, deleted.");
            continue;
          case QUOTE:
          case ESCAPE:
            fieldStringBuilder.append(ESCAPE);
        }

        fieldStringBuilder.append(c);
      }

      return fieldStringBuilder.toString();
    }

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

    public void setChangeSet(ChangeSet changeSet) {
      this.changeSet = changeSet;
    }

    public abstract void fillRowSet(RowSet rowSet) throws DbsyncException;

    public Provider getProvider() {
      return Provider.this;
    }

    public void addToRowSet(Row row, RowSet rowSet) throws DbsyncException {
      HashMap<String, ArrayList<Row>> bucket = rowSet.rowBucket;
      String mappedTable = row.mappedTable;
      ArrayList<Row> array = bucket.get(mappedTable);

      if (array == null) {
        try {
          array = cxt.idleRowArrays.borrowObject();
        } catch (Exception e) {
          String message = "Can't not borrow RowArray from the Object Pool.";
          throw new DbsyncException(message, e);
        }

        bucket.put(mappedTable, array);
      }

      array.add(row);
    }
  }
}
