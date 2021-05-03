package com.yg.inventory.data.db;

import com.yg.util.DB;
import com.yg.util.DB.DataType;
import com.yg.util.Java;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.Map;

public class SchemaAccess {

    static final String QEURY_FOR_COLUMNS      = Java.resource("QueryForColumns.sql");
    static final String QUERY_FOR_FOREIGN_KEYS = Java.resource("QueryForForeignKeys.sql");
    static final String QEURY_FOR_TABLE        = Java.resource("QueryForTables.sql");

    public Map<String, Map<String, DataType>> getTables() {
        return DB.query(QEURY_FOR_TABLE,
                ps -> {},
                rs -> new Tuple3<>(
                        rs.getString(1), // Table name
                        rs.getString(2), // Column name
                        DataType.DB_NAMES.get(rs.getString(3)).get()))
            .groupBy(t -> t._1)
            .mapValues(tl -> tl
                .groupBy(t -> t._2)
                .mapValues(cl -> cl.get()._3));
    }

    public Map<String, DataType> getTableColumns(String table) {
        return DB.query(QEURY_FOR_COLUMNS,
                ps -> ps.setString(1, table),
                rs -> new Tuple2<>(
                        rs.getString(1),
                        DataType.DB_NAMES.get(rs.getString(2)).get()))
            .toLinkedMap(t -> t);
    }

}
