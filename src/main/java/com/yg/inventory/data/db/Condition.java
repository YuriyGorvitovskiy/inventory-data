package com.yg.inventory.data.db;

import com.yg.util.DB;
import com.yg.util.Java;

import io.vavr.collection.List;
import io.vavr.collection.Seq;

public class Condition {
    public static final Condition NONE = new Condition(SQL.TRIVIAL, DB.Injects.NOTHING);

    public final String sql;

    public final DB.Inject inject;

    public Condition(String sql, DB.Inject inject) {
        this.sql = sql;
        this.inject = inject;
    }

    public static Condition and(List<Condition> child) {
        if (child.isEmpty()) {
            return NONE;
        }
        if (1 == child.size()) {
            return child.get();
        }
        return new Condition(
                child.map(c -> SQL.OPEN + c.sql + SQL.CLOSE).mkString(SQL.AND),
                DB.fold(child.map(c -> c.inject)));
    }

    public static Condition or(Condition... child) {
        return or(List.of(child));
    }

    public static Condition or(List<Condition> child) {
        if (child.isEmpty()) {
            return NONE;
        }
        if (1 == child.size()) {
            return child.get();
        }
        return new Condition(child.map(c -> SQL.OPEN + c.sql + SQL.CLOSE).mkString(SQL.OR), DB.fold(child.map(c -> c.inject)));
    }

    public static Condition not(Condition child) {
        return new Condition(SQL.NOT_OPEN + child.sql + SQL.CLOSE, child.inject);
    }

    public static Condition isNull(String column) {
        return new Condition(column + SQL.IS_NULL, DB.Injects.NOTHING);
    }

    public static Condition isNotNull(String column) {
        return new Condition(column + SQL.NOT_NULL, DB.Injects.NOTHING);
    }

    public static Condition equal(String column, DB.Inject value) {
        return new Condition(column + SQL.EQUAL + SQL.PARAM, value);
    }

    public static Condition equal(String columnLeft, String columnRight) {
        return new Condition(columnLeft + SQL.EQUAL + columnRight, DB.Injects.NOTHING);
    }

    public static Condition in(String column, Seq<DB.Inject> values) {
        return new Condition(column + SQL.IN_OPEN + Java.repeat(SQL.PARAM, SQL.COMMA, values.size()) + SQL.CLOSE,
                DB.fold(values));
    }

    public static Condition inArray(String column, DB.DataType elementType, DB.Inject array) {
        return new Condition(column + SQL.IN_OPEN + SQL.SELECT +
                SQL.UNNEST_PARAM_OPEN + DB.DataType.TO_DB_NAME.get(elementType).get() + SQL.ARRAY + SQL.CLOSE +
                SQL.CLOSE,
                array);
    }

    public static Condition textSearch(String column, Seq<String> values) {
        return new Condition(SQL.WEB_SEARCH + column,
                DB.Injects.Str.STRING.apply(values.mkString(" ")));
    }

}
