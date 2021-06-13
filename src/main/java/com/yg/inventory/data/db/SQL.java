package com.yg.inventory.data.db;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public interface SQL {
    public static final String AND               = " AND ";
    public static final String ARRAY             = "[]";
    public static final String AS_OPEN           = " AS (";
    public static final String ASC               = " ASC";
    public static final String BEETWEEN          = " BETWEEN ";
    public static final String CLOSE             = ")";
    public static final String COMMA             = ", ";
    public static final String CROSS_JOIN        = "CROSS JOIN ";
    public static final String DESC              = " DESC";
    public static final String DISTINCT          = "DISTINCT ";
    public static final String DOT               = ".";
    public static final String EQUAL             = " = ";
    public static final String FROM              = "FROM       ";
    public static final String FULL_JOIN         = "FULL  JOIN ";
    public static final String GREATER           = " > ";
    public static final String INDENT            = "    ";
    public static final String INNER_JOIN        = "INNER JOIN ";
    public static final String IN_OPEN           = " IN (";
    public static final String IS_NULL           = " IS NULL";
    public static final String LEFT_JOIN         = "LEFT  JOIN ";
    public static final String LESSER            = " < ";
    public static final String LIMIT             = "LIMIT ";
    public static final String NEXT_LINE         = "\n";
    public static final String NOT_BETWEEN       = " NOT BETWEEN ";
    public static final String NOT_EQUAL         = " != ";
    public static final String NOT_GREATER       = " <= ";
    public static final String NOT_IN_OPEN       = " NOT IN (";
    public static final String NOT_LESSER        = " >= ";
    public static final String NOT_NULL          = " IS NOT NULL";
    public static final String NOT_OPEN          = "NOT (";
    public static final String OFFSET            = "OFFSET ";
    public static final String ON                = " ON ";
    public static final String OPEN              = "(";
    public static final String OR                = " OR ";
    public static final String ORDER_BY          = "ORDER BY ";
    public static final String PARAM             = "?";
    public static final String RIGHT_JOIN        = "RIGHT JOIN ";
    public static final String SELECT            = "SELECT ";
    public static final String SPACE             = " ";
    public static final String TRIVIAL           = "1 = 1";
    public static final String UNNEST_PARAM_OPEN = "UNNEST((?)::";
    public static final String WEB_SEARCH        = "websearch_to_tsquery('english', ?) @@ ";
    public static final String WHERE             = "WHERE ";
    public static final String WITH              = "WITH ";

    public static final Map<Join.Kind, String> JOIN_KIND = HashMap.ofEntries(
            new Tuple2<>(Join.Kind.CROSS, CROSS_JOIN),
            new Tuple2<>(Join.Kind.FULL, FULL_JOIN),
            new Tuple2<>(Join.Kind.INNER, INNER_JOIN),
            new Tuple2<>(Join.Kind.LEFT, LEFT_JOIN),
            new Tuple2<>(Join.Kind.RIGHT, RIGHT_JOIN));

}
