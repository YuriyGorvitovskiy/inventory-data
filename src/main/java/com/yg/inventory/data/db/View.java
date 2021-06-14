package com.yg.inventory.data.db;

import com.yg.util.DB;
import com.yg.util.Java;
import com.yg.util.Tree;

import io.vavr.collection.List;

public class View<T> {

    public static class Node {
        public final String table;
        public final String alias;

        public Node(String table, String alias) {
            this.table = table;
            this.alias = alias;
        }
    }

    public final String                   name;
    public final List<Column<T>>          select;
    public final Tree<String, Node, Join> joins;
    public final Condition                where;
    public final List<Column<Boolean>>    order;
    public final boolean                  distinct;
    public final Long                     skip;
    public final Integer                  limit;

    public View(String name,
                Tree<String, Node, Join> joins,
                Condition where,
                List<Column<Boolean>> order,
                List<Column<T>> select,
                boolean distinct,
                Long skip,
                Integer limit) {
        this.name = name;
        this.joins = joins;
        this.where = where;
        this.order = order;
        this.select = select;
        this.distinct = distinct;
        this.skip = skip;
        this.limit = limit;
    }

    public static Tree<String, Node, Join> alias(String aliasPrefix, Tree<String, String, Join> joins) {
        return joins.mapNodesWithIndex(1, (n, i) -> new Node(n, aliasPrefix + i));
    }

    public String declare(int indentLength) {
        return name +
                SQL.SPACE +
                select.map(c -> c.name).mkString(SQL.OPEN, SQL.COMMA, SQL.CLOSE) +
                SQL.NEXT_LINE +
                sql(indentLength + 1) +
                Java.repeat(SQL.INDENT, indentLength) +
                SQL.CLOSE;
    }

    public String sql(int indentLength) {
        String indent = Java.repeat(SQL.INDENT, indentLength);

        StringBuilder sb = new StringBuilder()
            .append(indent)
            .append(SQL.SELECT);

        if (distinct) {
            sb.append(SQL.DISTINCT);
        }
        sb.append(select.map(c -> c.alias + SQL.DOT + c.column).mkString(SQL.COMMA))
            .append(SQL.NEXT_LINE);

        indent = indent + SQL.INDENT;
        joinSql(sb, indent);
        if (Condition.NONE != where) {
            sb.append(indent)
                .append(SQL.WHERE)
                .append(where.sql)
                .append(SQL.NEXT_LINE);
        }
        if (!order.isEmpty()) {
            sb.append(indent)
                .append(SQL.ORDER_BY)
                .append(order.map(c -> c.alias + SQL.DOT + c.column + (c._1 ? SQL.ASC : SQL.DESC)).mkString(SQL.COMMA))
                .append(SQL.NEXT_LINE);
        }
        if (null != limit) {
            sb.append(indent)
                .append(SQL.LIMIT)
                .append(limit)
                .append(SQL.NEXT_LINE);
        }
        if (null != skip) {
            sb.append(indent)
                .append(SQL.OFFSET)
                .append(skip)
                .append(SQL.NEXT_LINE);
        }
        return sb.toString();
    }

    public List<DB.Inject> injects() {
        return injectJoins(joins).append(where.inject);
    }

    List<DB.Inject> injectJoins(Tree<String, Node, Join> tree) {
        return tree.links
            .values()
            .flatMap(t -> List.of(t._1.condition.inject).appendAll(injectJoins(t._2)))
            .toList();
    }

    public String joinSql(StringBuilder sb, String indent) {
        sb.append(indent)
            .append(SQL.FROM)
            .append(joins.getNode().table)
            .append(SQL.SPACE)
            .append(joins.getNode().alias)
            .append(SQL.NEXT_LINE);

        return joins.links
            .values()
            .foldLeft(sb, (s, c) -> joinSql(s, indent, c._1, c._2))
            .toString();
    }

    StringBuilder joinSql(StringBuilder sb, String indent, Join join, Tree<String, Node, Join> right) {
        sb.append(indent)
            .append(SQL.JOIN_KIND.get(join.kind).get())
            .append(right.getNode().table)
            .append(SQL.SPACE)
            .append(right.getNode().alias)
            .append(SQL.ON)
            .append(join.condition.sql)
            .append(SQL.NEXT_LINE);

        return right.links
            .values()
            .foldLeft(sb, (s, c) -> joinSql(sb, indent, c._1, c._2));
    }

}
