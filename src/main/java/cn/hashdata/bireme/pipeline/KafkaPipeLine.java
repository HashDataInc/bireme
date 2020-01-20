package cn.hashdata.bireme.pipeline;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import cn.hashdata.bireme.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import com.codahale.metrics.Timer;

/**
 * {@code KafkaPipeLine} is a kind of {@code PipeLine} that polls data from Kafka.
 *
 * @author yuze
 */
public abstract class KafkaPipeLine extends PipeLine {
    private final long POLL_TIMEOUT = 100L;

    protected KafkaConsumer<String, String> consumer;
    protected LinkedBlockingQueue<KafkaCommitCallback> commitCallbacks;
    public LinkedBlockingQueue<Connection> loaderConnections;

    public KafkaPipeLine(Context cxt, SourceConfig conf, String myName) {
        super(cxt, conf, myName);
        consumer = KafkaPipeLine.createConsumer(conf.server, conf.groupID);
        commitCallbacks = new LinkedBlockingQueue<KafkaCommitCallback>();
    }

    @Override
    public ChangeSet pollChangeSet() throws BiremeException {
        ConsumerRecords<String, String> records = null;

        try {
            records = consumer.poll(POLL_TIMEOUT);
        } catch (InterruptException e) {
        }

        if (cxt.stop || records == null || records.isEmpty()) {
            return null;
        }

        KafkaCommitCallback callback = new KafkaCommitCallback();

        if (!commitCallbacks.offer(callback)) {
            String Message = "Can't add CommitCallback to queue.";
            throw new BiremeException(Message);
        }

        stat.recordCount.mark(records.count());

        return packRecords(records, callback);
    }

    @Override
    public void checkAndCommit() {
        CommitCallback callback = null;

        while (!commitCallbacks.isEmpty()) {
            if (commitCallbacks.peek().ready()) {
                callback = commitCallbacks.remove();
            } else {
                break;
            }
        }

        if (callback != null) {
            callback.commit();
        }
    }

    private ChangeSet packRecords(
            ConsumerRecords<String, String> records, KafkaCommitCallback callback) {
        ChangeSet changeSet = new ChangeSet();
        changeSet.createdAt = new Date();
        changeSet.changes = records;
        changeSet.callback = callback;

        return changeSet;
    }

    /**
     * Loop through the {@code ChangeSet} and transform each change data into a {@code Row}.
     *
     * @author yuze
     */
    public abstract class KafkaTransformer extends Transformer {
        @SuppressWarnings("unchecked")
        @Override
        public void fillRowSet(RowSet rowSet) throws BiremeException {
            CommitCallback callback = changeSet.callback;
            HashMap<String, Long> offsets = ((KafkaCommitCallback) callback).partitionOffset;
            Row row = null;

            /**
             * 循环遍历消费者记录，消费消息
             */
            for (ConsumerRecord<String, String> change :
                    (ConsumerRecords<String, String>) changeSet.changes) {
                row = new Row();

                if (!transform(change, row)) {
                    continue;
                }

                addToRowSet(row, rowSet);
                offsets.put(change.topic() + "+" + change.partition(), change.offset());
                callback.setNewestRecord(row.produceTime);

                /**
                 * 如果是ddl
                 */
                if (row.isDDL) {
                    /**
                     * 判断表是否存在，存在则count = 1, 否则count = 0
                     */
                    boolean tableExists = tableExists(row.table);

                    /**
                     * 判断ddl类型
                     */
                    switch (row.ddlType) {
                        /**
                         * 创建表
                         */
                        case CREATE_TABLE:
                            if (!tableExists) {
                                logger.info(createTable(row) ? "表{}创建成功!" : "表{}创建发生异常!", row.table, row.table);
                            }
                            break;
                        /**
                         * 删除表
                         */
                        case DROP_TABLE:
                            if (tableExists) {
                                logger.info(dropTable(row.table) ? "表{}删除成功!" : "表{}删除发生异常!", row.table, row.table);
                            }
                            break;
                        /**
                         * 修改表
                         */
                        case ALTER_TABLE:
                            if (tableExists) {
                                logger.info(alterTable(row, row.table + "_tmp") ? "表{}修改成功!" : "表{}修改发生异常!", row.table, row.table);
                            }
                            break;
                        /**
                         * 创建/删除/修改数据库，直接执行binlog中的sql
                         */
                        case CREATE_DATABASE:
                            if (!existsDatabase(row.database)) {
                                execDataBaseDDL(row);
                            }
                            break;
                        case DROP_DATABASE:
                        case ALTER_DATABASE:
                            if (existsDatabase(row.database)) {
                                execDataBaseDDL(row);
                            }
                            break;
                    }

                }
            }
            callback.setNumOfTables(rowSet.rowBucket.size());
            rowSet.callback = callback;
        }


        /**
         * 判断数据库是否存在
         * @auth zhuhai
         *
         * @param database 数据库名
         * @return true--存在
         */
        public boolean existsDatabase(String database) {
            Connection conn = null;
            try {
                conn = BiremeUtility.jdbcConn(cxt.conf.targetDatabase);
            } catch (BiremeException ex) {
                ex.printStackTrace();
            }
            PreparedStatement stmt = null;
            ResultSet rs = null;
            String sql = "SELECT count(1) FROM pg_catalog.pg_database u where u.datname=?";
            boolean exists = false;
            try {
                stmt = conn.prepareStatement(sql);
                stmt.setString(1, database);
                rs = stmt.executeQuery();
                rs.next();
                /**
                 * 判断库是否存在，存在则count = 1, 否则count = 0
                 */
                exists = rs.getInt(1) > 0;
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (null != rs) {
                        rs.close();
                    }
                    if (null != stmt) {
                        stmt.close();
                    }
                    if (null != conn) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            return exists;
        }

        /**
         * 执行对数据库的操作的ddl
         * @auth zhuhai
         *
         * @param row 1条消费的记录
         */
        public void execDataBaseDDL(Row row) {
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = BiremeUtility.jdbcConn(cxt.conf.targetDatabase);
                stmt = conn.createStatement();
                stmt.execute(row.ddlSQL);
            } catch (BiremeException ex) {
                ex.printStackTrace();
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (null != stmt) {
                        stmt.close();
                    }
                    if (null != conn) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }


        /**
         * 判断表是否存在
         * @auth zhuhai
         *
         * @param tableName 表名
         * @return 表是否存在
         */
        public boolean tableExists(String tableName) {
            Connection conn = null;
            try {
                conn = BiremeUtility.jdbcConn(cxt.conf.targetDatabase);
            } catch (BiremeException ex) {
                ex.printStackTrace();
            }
            PreparedStatement stmt = null;
            ResultSet rs = null;
            String sql = "select count(1) from information_schema.tables where table_schema=? and table_type=? and table_name=?";
            boolean exists = false;
            try {
                stmt = conn.prepareStatement(sql);
                stmt.setString(1, "public");
                stmt.setString(2, "BASE TABLE");
                stmt.setString(3, tableName);
                rs = stmt.executeQuery();
                rs.next();
                /**
                 * 判断表是否存在，存在则count = 1, 否则count = 0
                 */
                exists = rs.getInt(1) > 0;
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (null != rs) {
                        rs.close();
                    }
                    if (null != stmt) {
                        stmt.close();
                    }
                    if (null != conn) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            return exists;
        }

        /**
         * 删除表
         * @auth zhuhai
         *
         * @param tableName 表名
         * @return true 删除成功
         */
        public boolean dropTable(String tableName) {
            Connection conn = null;
            String sql = "drop table %s";
            String finalSQL = String.format(sql, tableName);
            Statement stmt = null;
            try {
                conn = BiremeUtility.jdbcConn(cxt.conf.targetDatabase);
                conn.setAutoCommit(false);
                stmt = conn.createStatement();
                stmt.execute(finalSQL);
                conn.commit();
            } catch (BiremeException ex) {
                ex.printStackTrace();
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (null != stmt) {
                        stmt.close();
                    }
                    if (null != conn) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            /**
             * 删除后判断表是否还存在
             */
            if (tableExists(tableName)) {
                return false;
            } else {
                return true;
            }
        }


        /**
         * 创建表
         * @auth zhuhai
         *
         * @param row 记录
         * @return 创建表是否成功
         */
        public boolean createTable(Row row) {
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = BiremeUtility.jdbcConn(cxt.conf.targetDatabase);
                //conn.setAutoCommit(false);
                stmt = conn.createStatement();
            } catch (BiremeException ex) {
                ex.printStackTrace();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            String sql = "create table %s (";
            String pkSql = "";
            String finalSQL = String.format(sql, row.table);
            List<Column> columns = row.def.getColumns();
            for (Column column : columns) {
                String type = column.getType();
                String name = column.getName();
                String newType = "";
                if ("int".equals(type)) {
                    newType = "int4";
                } else if ("varchar".equals(type)) {
                    newType = "varchar(100)";
                } else if ("char".equals(type)) {
                    newType = "char(2)";
                } else if ("datetime".equalsIgnoreCase(type)) {
                    newType = "timestamp";
                } else if ("timestamp".equalsIgnoreCase(type)) {
                    newType = "timestamptz";
                } else if ("longtext".equalsIgnoreCase(type)) {
                    newType = "text";
                } else if ("double".equalsIgnoreCase(type)) {
                    newType = "double precision";
                } else {
                    newType = type;
                }
                finalSQL = finalSQL + " \"" + name + "\" " + newType + ",";
            }
            finalSQL = finalSQL.substring(0, finalSQL.lastIndexOf(",")) + ");";
            logger.info("sql: " + finalSQL);
            List<String> primaryKeys = row.def.getPrimary_key();
            if (primaryKeys != null && primaryKeys.size() > 0) {
                String pk = "";
                //设置主键
                for (String primaryKey : primaryKeys) {
                    if (StringUtils.isEmpty(pk)) {
                        pk = primaryKey;
                    } else {
                        pk = pk + "," + primaryKey + "";
                    }
                }
                pkSql = "ALTER TABLE \"" + row.table + "\" ADD CONSTRAINT \"" + row.table + "_pkey\"" + " PRIMARY KEY (" + pk + ")";
                logger.info("alter_pk: " + pkSql);
            }

            try {
                stmt.execute(finalSQL);
                if (StringUtils.isNotEmpty(pkSql)) {
                    stmt.execute(pkSql);
                }
                //conn.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (null != stmt) {
                        stmt.close();
                    }
                    if (null != conn) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            return tableExists(row.table);
        }

        /**
         * 判断两个表是否数据同步
         * @auth zhuhai
         *
         * @param table1 第一个表
         * @param table2 第二个表
         * @return 是否同步
         */
        public boolean isSyncData(String table1, String table2) {
            boolean isSync = false;
            Connection conn = null;
            Statement pstmt = null;
            ResultSet rs = null;
            String sql = String.format("select * from (select count(1) from %s) cnt1, (select count(1) from %s) cnt2", table1, table2);
            try {
                conn = BiremeUtility.jdbcConn(cxt.conf.targetDatabase);
                pstmt = conn.createStatement();
                rs = pstmt.executeQuery(sql);
                if (rs.next()) {
                    if (rs.getInt(1) == rs.getInt(2)) {
                        isSync = true;
                    }
                }
            } catch (BiremeException ex) {
                ex.printStackTrace();
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (null != rs) {
                        rs.close();
                    }
                    if (null != pstmt) {
                        pstmt.close();
                    }
                    if (null != conn) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            return isSync;
        }


        /**
         * 修改表
         * 修改的策略是： 先将数据被分到临时表；然后删除表；最后再创建表并插入数据
         * @auth zhuhai
         *
         * @param row 记录
         * @param tmpTable 临时表
         * @return true: 修改成功
         */
        public boolean alterTable(Row row, String tmpTable) {
            boolean isAltered = false;
            Connection conn = null;
            try {
                conn = BiremeUtility.jdbcConn(cxt.conf.targetDatabase);
                conn.setAutoCommit(false);
            } catch (BiremeException ex) {
                ex.printStackTrace();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            Statement stmt = null;
            Statement insertData = null;
            Statement delTemp = null;
            String duplicate = String.format("select * into %s from %s", tmpTable, row.table);
            String insert = String.format("insert into %s select * from %s", row.table, tmpTable);
            String delTempTable = String.format("drop table %s", tmpTable);
            try {
                boolean tmpExists = false;
                if (tableExists(tmpTable)) {
                    if (dropTable(tmpTable)) {
                        tmpExists = false;
                    } else {
                        tmpExists = true;
                    }
                }

                /**
                 * 1. 备份数据
                 */
                /**
                 * 将数据被分到临时表
                 */
                stmt = conn.createStatement();
                if (!tmpExists) {
                    stmt.execute(duplicate);
                    conn.commit();
                }

                /**
                 *  2. 删除表
                 * 查询数据是否已经同步
                 */
                boolean isDeled = false;
                if (isSyncData(row.table, tmpTable)) {
                    if (tableExists(row.table)) {
                        isDeled = dropTable(row.table);
                    }
                }

                /**
                 * 3. 创建表并插入临时数据
                 */
                if (isDeled) {
                    boolean isCreated = createTable(row);
                    if (isCreated) {
                        insertData = conn.createStatement();
                        insertData.execute(insert);
                        conn.commit();
                        if (isSyncData(row.table, tmpTable)) {
                            isAltered = true;
                            /**
                             * 4. 删除临时表
                             */
                            delTemp = conn.createStatement();
                            delTemp.execute(delTempTable);
                            conn.commit();
                            if (tableExists(tmpTable)) {
                                logger.error("临时表\" {} \" 删除失败！", tmpTable);
                            }
                        }
                    }
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    closeStatement(stmt, insertData, delTemp);
                    if (null != conn) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            return isAltered;
        }

        /**
         * 关闭statement
         * @auth zhuhai
         *
         * @param statments statement数组
         */
        public void closeStatement(Statement... statments) {
            for (int i = 0; i < statments.length; i++) {
                try {
                    if (null != statments[i]) {
                        statments[i].close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }

        /**
         * Transform the change data into a {@code Row}.
         *
         * @param change the change data
         * @param row    an empty {@code Row} to store the result.
         * @return {@code true} if transform the change data successfully, {@code false} it the change
         * data is null or filtered
         * @throws BiremeException when can not get the field
         */
        public abstract boolean transform(ConsumerRecord<String, String> change, Row row)
                throws BiremeException;
    }

    /**
     * {@code KafkaCommitCallback} is used to trace a {@code ChangeSet} polled from Kafka. After the
     * change data has been applied, commit the offset to Kafka.
     *
     * @author yuze
     */
    public class KafkaCommitCallback extends AbstractCommitCallback {
        public HashMap<String, Long> partitionOffset;
        private Timer.Context timerCTX;
        private Date start;

        public KafkaCommitCallback() {
            this.partitionOffset = new HashMap<String, Long>();

            // record the time being created
            timerCTX = stat.avgDelay.time();
            start = new Date();
        }

        @Override
        public void commit() {
            HashMap<TopicPartition, OffsetAndMetadata> offsets =
                    new HashMap<TopicPartition, OffsetAndMetadata>();

            partitionOffset.forEach((key, value) -> {
                String topic = key.split("\\+")[0];
                int partition = Integer.valueOf(key.split("\\+")[1]);
                offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(value + 1));
            });

            consumer.commitSync(offsets);
            committed.set(true);
            partitionOffset.clear();

            // record the time being committed
            timerCTX.stop();

            stat.newestCompleted = newestRecord;
            stat.delay = new Date().getTime() - start.getTime();
        }

        @Override
        public void destory() {
            super.destory();
            partitionOffset.clear();
            partitionOffset = null;
            timerCTX = null;
            start = null;
        }

    }

    /**
     * Create a new KafkaConsumer, specify the server's ip and port, and groupID.
     *
     * @param server  ip and port for Kafka server
     * @param groupID consumer's group id
     * @return the consumer
     */
    public static KafkaConsumer<String, String> createConsumer(String server, String groupID) {
        Properties props = new Properties();
        props.put("bootstrap.servers", server);
        props.put("group.id", groupID);
        props.put("enable.auto.commit", false);
        props.put("session.timeout.ms", 60000);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<String, String>(props);
    }
}
