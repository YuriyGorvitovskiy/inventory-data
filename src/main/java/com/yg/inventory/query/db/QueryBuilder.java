package com.yg.inventory.query.db;

import com.yg.inventory.data.db.Condition;
import com.yg.inventory.data.db.Join;
import com.yg.inventory.data.db.SQL;
import com.yg.inventory.data.db.View;
import com.yg.inventory.data.db.View.Node;
import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;
import com.yg.util.Tree;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.control.Option;

public class QueryBuilder {
    final Schema schema;

    QueryBuilder(Schema schema) {
        this.schema = schema;
    }

    public View<Void> filter(String tableName, List<Comparison> conditions) {
        Table                                            table = schema.tables.get(tableName).get();
        Tree<String, Table, Tuple2<ForeignKey, Boolean>> joins = conditions.foldLeft(Tree.of(table),
                (j, c) -> j.putIfMissed(c.path.dropRight(1), (t, n) -> createJoin(t, n, c)));

        Tree<String, Node, Tuple2<ForeignKey, Boolean>> joins2 = joins.mapNodesWithIndex(1,
                (t, i) -> new Node(t.name, "f" + i));

        Tree<String, Node, Join> joins3 = joins2.mapLinksWithNodes((t) -> createJoin(t._1.alias, t._3.alias, t._2._1, t._2._2));

        Condition                             where = createWhere(joins3, conditions);
        com.yg.inventory.data.db.Column<Void> id    = new com.yg.inventory.data.db.Column<Void>(joins3.node.alias,
                "id",
                "id",
                null);

        return new View<Void>("filter", joins3, where, List.empty(), List.of(id), true, null, null);
    }

    Tuple2<Tuple2<ForeignKey, Boolean>, Table> createJoin(Table table, String columnName, Comparison condition) {
        Option<ForeignKey> out = table.outgoing.get(columnName);
        if (out.isDefined()) {
            return new Tuple2<>(new Tuple2<>(out.get(), true), schema.tables.get(out.get().toTable).get());
        }
        Option<ForeignKey> in = table.incoming.get(columnName);
        if (in.isDefined()) {
            return new Tuple2<>(new Tuple2<>(in.get(), false), schema.tables.get(in.get().fromTable).get());
        }
        throw new RuntimeException("Unknown relation: " + columnName);
    }

    Join createJoin(String aliasLeft, String aliasRight, ForeignKey foreignKey, boolean oneToOne) {
        String left  = aliasLeft + SQL.DOT + (oneToOne ? foreignKey.fromColumn : foreignKey.toColumn);
        String right = aliasRight + SQL.DOT + (oneToOne ? foreignKey.toColumn : foreignKey.fromColumn);
        return new Join(Join.Kind.LEFT, Condition.equal(left, right), oneToOne);
    }

    Condition createWhere(Tree<String, Node, Join> joins, List<Comparison> comparisons) {
        List<Condition> conditions = comparisons.map((c) -> createCondition(joins.getNode(c.path.dropRight(1)).get(), c));
        return Condition.and(conditions);
    }

    Condition createCondition(Node node, Comparison comparison) {
        Table  table  = schema.tables.get(node.table).get();
        Column column = table.columns.get(comparison.path.last()).get();
        String alias  = node.alias + SQL.DOT + column.name;

        Tuple2<String, DB.Inject> condition = DB.conditionSqlInject(alias, column.type, DB.DATA_TYPE_INJECT, comparison.value);
        return new Condition(condition._1, condition._2);
    }
}
