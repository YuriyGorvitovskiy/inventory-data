package com.yg.inventory.data.db;

import com.yg.util.DB;

import io.vavr.Tuple2;
import io.vavr.collection.List;

public class Filter {

    public final String       column;
    public final DB.DataType  dataType;
    public final List<Object> values;

    public Filter(String column, DB.DataType dataType, List<Object> values) {
        this.column = column;
        this.dataType = dataType;
        this.values = values;
    }

    public Tuple2<String, DB.Inject> sql(String alias) {
        return DB.conditionSqlInject(alias + "." + column, dataType, DB.DATA_TYPE_INJECT, values);
    }
}
