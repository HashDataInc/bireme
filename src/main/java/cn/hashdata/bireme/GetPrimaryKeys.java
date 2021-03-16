package cn.hashdata.bireme;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zuanqi
 */
public class GetPrimaryKeys {
    private static final Logger logger = LogManager.getLogger(GetPrimaryKeys.class);

    public static Map<String, List<String>> getPrimaryKeys(
            HashMap<String, String> tableMap, Connection conn) throws Exception {
        Statement statement = null;
        ResultSet resultSet = null;
        ResultSet tableRs = null;
        Map<String, List<String>> tablePRMap = new HashMap<>();
        List<String> checkTableMap = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("(");

        for (String fullname : tableMap.values()) {
            sb.append("'").append(fullname).append("',");
        }

        String tableList = sb.substring(0, sb.toString().length() - 1) + ")";
        String tableSql = "select tablename from pg_tables where schemaname || '.' || tablename in "
                + tableList + "";
        String prSql = "SELECT NULL AS TABLE_CAT, "
                + "n.nspname  AS TABLE_SCHEM, "
                + "ct.relname AS TABLE_NAME, "
                + "a.attname  AS COLUMN_NAME, "
                + "(i.keys).n AS KEY_SEQ, "
                + "ci.relname AS PK_NAME "
                + "FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) "
                + "JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
                + "JOIN ( SELECT i.indexrelid, i.indrelid, i.indisprimary, information_schema._pg_expandarray(i.indkey) AS KEYS FROM pg_catalog.pg_index i) i ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) "
                + "JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE TRUE AND n.nspname || '.' || ct.relname in "
                + tableList + " AND i.indisprimary ORDER BY TABLE_NAME, pk_name, key_seq";
        try {
            statement = conn.createStatement();

            logger.info("tableSql: {}", tableSql);
            tableRs = statement.executeQuery(tableSql);
            while (tableRs.next()) {
                checkTableMap.add(tableRs.getString("tablename"));
            }
            if (checkTableMap.size() != tableMap.size()) {
                String message = "Greenplum table and source table are inconsistent!";
                throw new BiremeException(message);
            } else {
                logger.info("Greenplum table is checked. The state is okay！");
            }

            logger.info("tablePRSql: {}", prSql);
            resultSet = statement.executeQuery(prSql);
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_SCHEM") + '.' + resultSet.getString("TABLE_NAME");
                if (tablePRMap.containsKey(tableName)) {
                    List<String> strings = tablePRMap.get(tableName);
                    strings.add(resultSet.getString("COLUMN_NAME"));
                } else {
                    List<String> multiPKList = new ArrayList<>();
                    multiPKList.add(resultSet.getString("COLUMN_NAME"));
                    tablePRMap.put(tableName, multiPKList);
                }
            }

            // TODO:是否要校验主键, 倒是未必, 有待后期再完善
            if (tablePRMap.size() != tableMap.size()) {
                String message = "some tables do not have primary keys！";
                throw new BiremeException(message);
            } else {
                logger.info("Greenplum table primary key check is completed, the state is okay！");
            }

        } catch (SQLException e) {
            try {
                statement.close();
                conn.close();
            } catch (SQLException ignore) {
            }
            String message = "Could not get PrimaryKeys";
            throw new BiremeException(message, e);
        }

        return tablePRMap;
    }
}
