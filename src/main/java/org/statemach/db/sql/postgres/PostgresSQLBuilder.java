package org.statemach.db.sql.postgres;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.jdbc.Vendor;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.DataType;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.SQL;
import org.statemach.db.sql.SQLBuilder;
import org.statemach.db.sql.Select;
import org.statemach.db.sql.TableLike;
import org.statemach.db.sql.View;
import org.statemach.util.Java;
import org.statemach.util.Json;
import org.statemach.util.NodeLinkTree;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import io.vavr.collection.Traversable;

public class PostgresSQLBuilder implements SQLBuilder {

    final String schema;

    PostgresSQLBuilder(String schema) {
        this.schema = schema;
    }

    @Override
    public Vendor getVendor() {
        return Vendor.POSTGRES;
    }

    @Override
    public Condition and(List<Condition> child) {
        if (child.isEmpty()) {
            return Condition.NONE;
        }
        if (1 == child.size()) {
            return child.get();
        }
        return new Condition(
                child.map(c -> SQL.OPEN + c.sql + SQL.CLOSE).mkString(SQL.AND),
                Inject.fold(child.map(c -> c.inject)));
    }

    @Override
    public Condition or(List<Condition> child) {
        if (child.isEmpty()) {
            return Condition.NONE;
        }
        if (1 == child.size()) {
            return child.get();
        }
        return new Condition(
                child.map(c -> SQL.OPEN + c.sql + SQL.CLOSE).mkString(SQL.OR),
                Inject.fold(child.map(c -> c.inject)));
    }

    @Override
    public Condition not(Condition child) {
        return new Condition(SQL.NOT_OPEN + child.sql + SQL.CLOSE, child.inject);
    }

    @Override
    public Condition isNull(Select<?> column) {
        return new Condition(column.sql() + SQL.IS_NULL, Inject.NOTHING);
    }

    @Override
    public Condition isNotNull(Select<?> column) {
        return new Condition(column.sql() + SQL.NOT_NULL, Inject.NOTHING);
    }

    @Override
    public Condition equal(Select<?> column, Inject value) {
        return new Condition(column.sql() + SQL.EQUAL + SQL.PARAM, value);
    }

    @Override
    public Condition equal(Select<?> left, Select<?> right) {
        return new Condition(left.sql() + SQL.EQUAL + right.sql(), Inject.NOTHING);
    }

    @Override
    public Condition in(Select<?> column, Traversable<Inject> values) {
        return new Condition(column.sql() + SQL.IN_OPEN + Java.repeat(SQL.PARAM, SQL.COMMA, values.size()) + SQL.CLOSE,
                Inject.fold(values));
    }

    @Override
    public Condition inArray(Select<?> column, DataType elementType, Traversable<?> array) {
        return new Condition(column.sql() + SQL.IN_OPEN + SQL.SELECT +
                SQL.UNNEST_PARAM_OPEN + elementType.name + SQL.ARRAY + SQL.CLOSE +
                SQL.CLOSE,
                Inject.ARRAY.apply(elementType, array));
    }

    @Override
    public Condition textSearch(Select<?> column, Traversable<String> values) {
        return new Condition(SQL.WEB_SEARCH + column.sql(),
                Inject.STRING.apply(values.mkString(" ")));
    }

    String querySql(List<View<String>> commonTableExpressions, View<Tuple2<String, Extract<?>>> query) {
        StringBuilder sb       = new StringBuilder();
        int           indent   = 0;
        Set<String>   cteNames = commonTableExpressions.map(c -> c.name).toSet();
        if (!commonTableExpressions.isEmpty()) {
            sb.append(SQL.WITH);
            sb.append(commonTableExpressions.map(v -> commonTableExrpressionSql(v, cteNames, 1)).mkString(SQL.COMMA));
            sb.append(SQL.NEXT_LINE);
            indent = 1;
        }
        sb.append(viewSql(query, cteNames, indent));
        return sb.toString();
    }

    String commonTableExrpressionSql(View<String> v, Set<String> cteNames, int indentLength) {
        return v.name +
                SQL.SPACE +
                v.select.map(c -> c._1).mkString(SQL.OPEN, SQL.COMMA, SQL.CLOSE) +
                SQL.AS_OPEN + SQL.NEXT_LINE +
                viewSql(v, cteNames, indentLength + 1) +
                Java.repeat(SQL.INDENT, indentLength) +
                SQL.CLOSE;
    }

    String viewSql(View<?> v, Set<String> cteNames, int indentLength) {
        String indent = Java.repeat(SQL.INDENT, indentLength);

        StringBuilder sb = new StringBuilder()
            .append(indent)
            .append(SQL.SELECT);

        if (v.distinct) {
            sb.append(SQL.DISTINCT);
        }

        sb.append(v.select.map(c -> c.from + SQL.DOT + c.column).mkString(SQL.COMMA))
            .append(SQL.NEXT_LINE);

        indent = indent + SQL.INDENT;
        joinSql(v.joins, cteNames, sb, indent);
        if (Condition.NONE != v.where) {
            sb.append(indent)
                .append(SQL.WHERE)
                .append(v.where.sql)
                .append(SQL.NEXT_LINE);
        }
        if (!v.order.isEmpty()) {
            sb.append(indent)
                .append(SQL.ORDER_BY)
                .append(v.order.map(c -> c.from + SQL.DOT + c.column + (c._1 ? SQL.ASC : SQL.DESC)).mkString(SQL.COMMA))
                .append(SQL.NEXT_LINE);
        }
        if (null != v.limit) {
            sb.append(indent)
                .append(SQL.LIMIT)
                .append(v.limit)
                .append(SQL.NEXT_LINE);
        }
        if (null != v.skip) {
            sb.append(indent)
                .append(SQL.OFFSET)
                .append(v.skip)
                .append(SQL.NEXT_LINE);
        }
        return sb.toString();
    }

    StringBuilder joinSql(NodeLinkTree<String, From, Join> joins, Set<String> cteNames, StringBuilder sb, String indent) {
        sb.append(indent)
            .append(SQL.FROM)
            .append(joins.getNode().table.sql)
            .append(SQL.SPACE)
            .append(joins.getNode().alias)
            .append(SQL.NEXT_LINE);

        return joins.links
            .values()
            .foldLeft(sb, (s, c) -> joinSql(c._1, c._2, cteNames, s, indent));
    }

    StringBuilder joinSql(Join join,
                          NodeLinkTree<String, From, Join> right,
                          Set<String> cteNames,
                          StringBuilder sb,
                          String indent) {
        sb.append(indent)
            .append(join.kind.sql)
            .append(right.getNode().table.sql)
            .append(SQL.SPACE)
            .append(right.getNode().alias)
            .append(SQL.ON)
            .append(join.condition.sql)
            .append(SQL.NEXT_LINE);

        return right.links
            .values()
            .foldLeft(sb, (s, c) -> joinSql(c._1, c._2, cteNames, sb, indent));
    }

    @Override
    public TableLike arrayAsTable(DataType type, List<ColumnInfo> columns, Traversable<Map<String, Object>> values) {
        // JSON_POPULATE_RECORDSET(null::scm.type, ?::JSON)
        String sql = SQL.JSON_POPULATE_RS_OPEN + schema + SQL.DOT + type.name
                + SQL.COMMA + SQL.PARAM + SQL.JSON_CAST
                + SQL.CLOSE;

        String json   = Java.soft(() -> Json.MAPPER.writeValueAsString(values));
        Inject inject = Inject.STRING.apply(json);

        return TableLike.of(sql, inject);
    }

    @Override
    public TableLike arrayAsTable(ColumnInfo column, Traversable<Object> values) {
        // (SELECT UNNEST((?)::type) AS col)
        String sql = SQL.OPEN
                + SQL.SELECT + SQL.UNNEST_PARAM_OPEN + column.type.name + SQL.ARRAY + SQL.CLOSE
                + SQL.AS + column.name
                + SQL.CLOSE;

        return TableLike.of(sql, Inject.ARRAY.apply(column.type, values));
    }
}
