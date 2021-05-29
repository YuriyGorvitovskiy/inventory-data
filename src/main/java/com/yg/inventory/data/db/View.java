package com.yg.inventory.data.db;

import com.yg.util.Java;
import com.yg.util.Tree;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class View<T> {

    public static class Column<T> {
        public final List<String> path;
        public final T            _1;

        public Column(List<String> path, T _1) {
            this.path = path;
            this._1 = _1;
        }
    }

    public static class Node {
        final String table;
        final String alias;

        public Node(String table, String alias) {
            this.table = table;
            this.alias = alias;
        }
    }

    public static final String FROM       = "FROM       ";
    public static final String CROSS_JOIN = "CROSS JOIN ";
    public static final String FULL_JOIN  = "FULL  JOIN ";
    public static final String INNER_JOIN = "INNER JOIN ";
    public static final String LEFT_JOIN  = "LEFT  JOIN ";
    public static final String RIGHT_JOIN = "RIGHT JOIN ";

    public static final String ON        = " ON ";
    public static final String EQUAL     = " = ";
    public static final String SPACE     = "";
    public static final String NEXT_LINE = "\n";

    public static final Map<Join.Kind, String> JOIN_KIND_SQL = HashMap.ofEntries(
            new Tuple2<>(Join.Kind.CROSS, CROSS_JOIN),
            new Tuple2<>(Join.Kind.FULL, FULL_JOIN),
            new Tuple2<>(Join.Kind.INNER, INNER_JOIN),
            new Tuple2<>(Join.Kind.LEFT, LEFT_JOIN),
            new Tuple2<>(Join.Kind.RIGHT, RIGHT_JOIN));

    public final Tree<String, Node, Join> joins;
    public final List<Column<T>>          columns;

    View(Tree<String, Node, Join> joins, List<Column<T>> columns) {
        this.joins = joins;
        this.columns = columns;
    }

    public static <T> View<T> of(String aliasPrefix, Tree<String, String, Join> joins, List<Column<T>> columns) {
        return new View<>(alias(aliasPrefix, joins), columns);
    }

    public static Tree<String, Node, Join> alias(String aliasPrefix, Tree<String, String, Join> joins) {
        return alias(aliasPrefix, 1, joins)._1;
    }

    static Tuple2<Tree<String, Node, Join>, Integer> alias(String aliasPrefix,
                                                           Integer i,
                                                           Tree<String, String, Join> joins) {
        Tuple2<Map<String, Tuple2<Join, Tree<String, Node, Join>>>, Integer> children = joins.links
            .foldLeft(new Tuple2<>(HashMap.empty(), i + 1), (a, l) -> {
                Tuple2<Tree<String, Node, Join>, Integer> t = alias(aliasPrefix, a._2, l._2._2);
                return new Tuple2<>(a._1.put(l._1, new Tuple2<>(l._2._1, t._1)), t._2);
            });

        return new Tuple2<>(Tree.of(new Node(joins.node, aliasPrefix + i), children._1), children._2);
    }

    public String joinSql(int indentLength) {
        StringBuilder sb     = new StringBuilder();
        String        indent = Java.repeat(" ", indentLength);
        String        alias  = joins.getNode().alias;
        sb.append(indent);
        sb.append(FROM);
        sb.append(joins.getNode().table);
        sb.append(SPACE);
        sb.append(alias);
        sb.append(NEXT_LINE);

        return joins.links
            .values()
            .foldLeft(sb, (s, c) -> joinSql(s, indent, alias, c._1, c._2))
            .toString();
    }

    StringBuilder joinSql(StringBuilder sb, String indent, String leftAlias, Join join, Tree<String, Node, Join> right) {
        String rightAlias = right.getNode().alias;
        sb.append(indent);
        sb.append(JOIN_KIND_SQL.get(join.kind).get());
        sb.append(right.getNode().table);
        sb.append(SPACE);
        sb.append(rightAlias);
        sb.append(ON);
        sb.append(rightAlias);
        sb.append(join.rightColumn);
        sb.append(EQUAL);
        sb.append(leftAlias);
        sb.append(join.leftColumn);
        sb.append(NEXT_LINE);

        return right.links
            .values()
            .foldLeft(sb, (s, c) -> joinSql(sb, indent, rightAlias, c._1, c._2));
    }
}
