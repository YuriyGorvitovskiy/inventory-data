package com.yg.inventory.data.db;

import com.yg.util.DB;
import com.yg.util.Java;

import io.vavr.collection.List;

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
        return new Condition(child.mkString(SQL.AND), DB.fold(child.map(c -> c.inject)));
    }

    public static Condition or(List<Condition> child) {
        if (child.isEmpty()) {
            return NONE;
        }
        if (1 == child.size()) {
            return child.get();
        }
        return new Condition(child.mkString(SQL.OR), DB.fold(child.map(c -> c.inject)));
    }

    public static Condition not(Condition child) {
        return new Condition(SQL.NOT_OPEN + child.sql + SQL.CLOSE, child.inject);
    }

    public static Condition isNull(String column) {
        return new Condition(column + SQL.IS_NULL, DB.Injects.NOTHING);
    }

    public static Condition equal(String column, DB.Inject value) {
        return new Condition(column + SQL.EQUAL + SQL.PARAM, value);
    }

    public static Condition equal(String columnLeft, String columnRight) {
        return new Condition(columnLeft + SQL.EQUAL + columnRight, DB.Injects.NOTHING);
    }

    public static Condition in(String column, List<DB.Inject> values) {
        return new Condition(column + SQL.IN_OPEN + Java.repeat(SQL.PARAM, SQL.COMMA, values.size()) + SQL.CLOSE,
                DB.Injects.NOTHING);
    }

    public static Condition textSearch(String column, List<String> values) {
        return new Condition(SQL.WEB_SEARCH + column,
                DB.Injects.Str.STRING.apply(values.mkString(" ")));
    }

}
