package cn.hashdata.bireme;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/***
     *
     *  * "def":{"database":"test","charset":"utf8mb4","table":"ddd",
     *  *          "columns":[{"type":"int","name":"id","signed":true},
     *  *                     {"type":"varchar","name":"daemon","charset":"utf8mb4"}],"primary-key":["id"]},
     *
     */
@Slf4j
@Getter
@Setter
@NoArgsConstructor
public class TableSchema {


    private String database;

    private String table;

    private String charset;

    private List<Column> columns;

    private List<String> primary_key;

}