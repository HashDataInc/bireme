package cn.hashdata.bireme.pipeline;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import cn.hashdata.bireme.*;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;

import cn.hashdata.bireme.Row.RowType;
import cn.hashdata.bireme.Row.DDLType;

/**
 * {@code MaxwellPipeLine} is a kind of {@code KafkaPipeLine} whose change data coming from Maxwell.
 *
 * @author yuze
 */
public class MaxwellPipeLine extends KafkaPipeLine {
    public MaxwellPipeLine(Context cxt, SourceConfig conf, int id) {
        super(cxt, conf, "Maxwell-" + conf.name + "-" + conf.topic + "-" + id);
        consumer.subscribe(Arrays.asList(conf.topic));
        logger = LogManager.getLogger("Bireme." + myName);
        logger.info("Create new Maxwell Pipeline. Name: {}", myName);
    }

    @Override
    public Transformer createTransformer() {
        return new MaxwellTransformer();
    }

    /**
     * {@code MaxwellChangeTransformer} is a type of {@code Transformer}. It is used to transform data
     * to {@code Row} from <B>Maxwell</B> data source.
     *
     * @author yuze
     */
    class MaxwellTransformer extends KafkaTransformer {
        HashMap<String, String> tableMap;

        public MaxwellTransformer() {
            super();
            tableMap = conf.tableMap;
        }

        private String getMappedTableName(MaxwellRecord record) {
            return cxt.tableMap.get(record.dataSource + "." + record.database + "." + record.table);
        }

        private String getOriginTableName(MaxwellRecord record) {
            return record.dataSource + "." + record.database + "." + record.table;
        }

        private boolean filter(MaxwellRecord record) {
            String fullTableName = record.dataSource + "." + record.database + "." + record.table;

            if (!tableMap.containsKey(fullTableName)) {
                return true;
            }

            return false;
        }

        @Override
        protected byte[] decodeToBinary(String data) {
            byte[] decoded = null;
            decoded = Base64.decodeBase64(data);
            return decoded;
        }

        @Override
        protected String decodeToBit(String data, int precision) {
            String binaryStr = Integer.toBinaryString(Integer.valueOf(data));
            return String.format("%" + precision + "s", binaryStr).replace(' ', '0');
        }

        @Override
        public boolean transform(ConsumerRecord<String, String> change, Row row)
                throws BiremeException {
            MaxwellRecord record = new MaxwellRecord(change.value());

            /**
             * ddlType为空，即非ddl才需要过滤
             */
            if (null == record.ddlType) {
                if (filter(record)) {
                    return false;
                }
            }

            row.type = record.type;
            row.ddlType = record.ddlType;
            row.produceTime = record.produceTime;
            row.originTable = getOriginTableName(record);
            row.mappedTable = getMappedTableName(record);
            row.ddlSQL = record.ddlSQL;
            row.def = record.def;
            row.database = record.database;
            row.table = record.table;
            row.isDDL = record.isDDL;

            if (row.ddlType == null || row.ddlSQL == null) {
                Table table = cxt.tablesInfo.get(getMappedTableName(record));
                row.keys = formatColumns(record, table, table.keyNames, false);

                /**
                 * 如果是插入或更新
                 */
                if (row.type == RowType.INSERT || row.type == RowType.UPDATE) {
                    row.tuple = formatColumns(record, table, table.columnName, false);
                }

                /**
                 * 如果是更新还需要old信息
                 */
                if (row.type == RowType.UPDATE) {
                    row.oldKeys = formatColumns(record, table, table.keyNames, true);

                    if (row.keys.equals(row.oldKeys)) {
                        row.oldKeys = null;
                    }
                }
            }
            return true;
        }

        class MaxwellRecord implements Record {
            public String dataSource;
            public String database;
            public String table;
            public Long produceTime;
            public RowType type;
            public JSONObject data;
            public JSONObject old;

            /**
             * ddl 数据定义
             *
             * @param changeValue
             */
            public DDLType ddlType;
            public String ddlSQL;
            public TableSchema def;
            public boolean isDDL = false;

            /**
             * 构造函数
             *
             * @param changeValue
             */
            public MaxwellRecord(String changeValue) {
                JSONObject value = JSONObject.parseObject(changeValue);

                this.dataSource = getPipeLineName();
                this.database = value.getString("database");
                this.table = value.getString("table");
                this.produceTime = value.getLong("ts") * 1000;
                /**
                 * 当data不存在时，应该赋一个空json，否则会报空指针异常
                 */
                if (value.containsKey("data")) {
                    this.data = value.getJSONObject("data");
                } else {
                    this.data = new JSONObject();
                }

                if (value.containsKey("old") && !value.getJSONObject("old").isEmpty()) {
                    this.old = value.getJSONObject("old");
                }

                /**
                 * 获取ddlSQL
                 */
                if (value.containsKey("sql") && !value.getString("sql").isEmpty()) {
                    this.ddlSQL = value.getString("sql");
                }
                if (value.containsKey("def") && !value.getJSONObject("def").isEmpty()) {
                    /**
                     * 获取表字段(列)
                     */
                    JSONObject defObject = value.getJSONObject("def");
                    TableSchema tableSchema = new TableSchema();
                    tableSchema.setDatabase(defObject.getString("database"));
                    tableSchema.setTable(defObject.getString("table"));
                    tableSchema.setCharset(defObject.getString("charset"));
                    JSONArray columnsJson = defObject.getJSONArray("columns");
                    List<String> primaryKeysList = new ArrayList<String>();
                    if (defObject.containsKey("primary-key")) {
                        String primaryKeyStr = defObject.get("primary-key").toString();
                        logger.info(this.table+"-->主键: " + primaryKeyStr);
                        if (!primaryKeyStr.isEmpty()) {
                            if (primaryKeyStr.contains("[")) {
                                primaryKeyStr = primaryKeyStr.replaceAll("\\[|\\]", "");
                                if (primaryKeyStr.contains(",")) {
                                    String[] primaryKeyArr = primaryKeyStr.split(",");
                                    for (int i = 0; i < primaryKeyArr.length; i++) {
                                        primaryKeysList.add(primaryKeyArr[i]);
                                    }
                                } else {
                                    primaryKeysList.add(primaryKeyStr);
                                }
                            } else {
                                primaryKeysList.add(primaryKeyStr);
                            }
                        }
                    }
                    List<Column> columns = new ArrayList<Column>();
                    try {
                        for (Object objetc : columnsJson) {
                            Column column = new Column();
                            BeanUtils.copyProperties(column, objetc);
                            columns.add(column);
                        }
                    } catch (IllegalAccessException ex) {
                        ex.printStackTrace();
                    } catch (InvocationTargetException ex) {
                        ex.printStackTrace();
                    }
                    tableSchema.setColumns(columns);
                    tableSchema.setPrimary_key(primaryKeysList);
                    this.def = tableSchema;
                }

                switch (value.getString("type")) {
                    case "insert":
                        type = RowType.INSERT;
                        break;
                    case "update":
                        type = RowType.UPDATE;
                        break;
                    case "delete":
                        type = RowType.DELETE;
                        break;
                    case "database-create":
                        isDDL = true;
                        ddlType = DDLType.CREATE_DATABASE;
                        break;
                    case "database-drop":
                        isDDL = true;
                        ddlType = DDLType.DROP_DATABASE;
                        break;
                    case "database-alter":
                        isDDL = true;
                        ddlType = DDLType.ALTER_DATABASE;
                        break;
                    case "table-create":
                        isDDL = true;
                        ddlType = DDLType.CREATE_TABLE;
                        break;
                    case "table-drop":
                        isDDL = true;
                        ddlType = DDLType.DROP_TABLE;
                        break;
                    case "table-alter":
                        isDDL = true;
                        ddlType = DDLType.ALTER_TABLE;
                        break;
                }
            }

            @Override
            public String getField(String fieldName, boolean oldValue) throws BiremeException {
                String field = null;

                if (oldValue) {
                    try {
                        field = BiremeUtility.jsonGetIgnoreCase(old, fieldName);
                        return field;
                    } catch (BiremeException ignore) {
                    }
                }

                return BiremeUtility.jsonGetIgnoreCase(data, fieldName);
            }
        }
    }
}
