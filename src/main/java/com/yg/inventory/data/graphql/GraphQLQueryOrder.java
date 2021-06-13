package com.yg.inventory.data.graphql;

import java.util.Objects;

import com.yg.inventory.data.db.Join;
import com.yg.inventory.data.db.View;
import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;
import com.yg.util.Tree;

import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.control.Option;

public class GraphQLQueryOrder {

    static interface Sort {
        static final String ASC  = "ASC";
        static final String DESC = "DESC";
    }

    static final String               SORTING_ORDER_TYPE_NAME = "SortingOrder";
    static final GraphQLTypeReference SORTING_ORDER_TYPE_REF  = GraphQLTypeReference.typeRef(SORTING_ORDER_TYPE_NAME);

    final Schema        schema;
    final GraphQLNaming naming;

    public GraphQLQueryOrder(Schema schema, GraphQLNaming naming) {
        this.schema = schema;
        this.naming = naming;
    }

    public List<GraphQLType> buildAllTypes() {
        return schema.tables.values()
            .map(this::buildType)
            .append(buildSortingOrderEnum())
            .toList();
    }

    GraphQLType buildType(Table table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getOrderTypeName(table.name))
            .fields(buildFields(table))
            .build();
    }

    GraphQLType buildSortingOrderEnum() {
        return GraphQLEnumType.newEnum()
            .name(SORTING_ORDER_TYPE_NAME)
            .value(Sort.ASC)
            .value(Sort.DESC)
            .build();
    }

    java.util.List<GraphQLInputObjectField> buildFields(Table table) {
        return table.columns.values()
            .filter(this::isOrderableColumn)
            .map(c -> buildField(c, table.outgoing.get(c.name)))
            .toJavaList();
    }

    boolean isOrderableColumn(Column column) {
        return DB.DataType.TEXT_SEARCH_VECTOR != column.type;
    }

    GraphQLInputObjectField buildField(Column column, Option<ForeignKey> outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(column.name)
            .type(outgoing.isDefined()
                    ? naming.getOrderTypeRef(outgoing.get().toTable)
                    : SORTING_ORDER_TYPE_REF)
            .build();
    }

    public List<OrderBy> parse(Table table, Object argument) {
        return parse(List.empty(), table, argument);
    }

    @SuppressWarnings("unchecked")
    List<OrderBy> parse(List<String> path, Table table, Object argument) {
        if (!(argument instanceof java.util.Map)) {
            return List.empty();
        }

        return HashMap.ofAll((java.util.Map<String, Object>) argument)
            .flatMap(t -> parse(path, table, t._1, t._2))
            .toList();
    }

    List<OrderBy> parse(List<String> path, Table table, String field, Object value) {
        Option<ForeignKey> outgoing = table.outgoing.get(field);
        if (outgoing.isDefined()) {
            Option<Table> join = schema.tables.get(outgoing.get().toTable);
            if (join.isEmpty()) {
                return List.empty();
            }
            return parse(path.append(field), join.get(), value);
        }

        Option<Column> column = table.columns.get(field);
        if (column.isDefined()) {
            return List.of(new OrderBy(path, Sort.DESC.equalsIgnoreCase(Objects.toString(value))));
        }
        return List.empty();
    }

    public Tree<String, Table, ForeignKeyJoin> buildJoins(Tree<String, Table, ForeignKeyJoin> tree, List<OrderBy> sorting) {
        return sorting.foldLeft(tree,
                (t, s) -> t.putIfMissed(s.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.LEFT)));
    }

    Tuple2<ForeignKeyJoin, Table> buildJoin(Table parent, String step, Join.Kind join) {
        ForeignKey outgoing = parent.outgoing.get(step).get();
        return new Tuple2<>(new ForeignKeyJoin(join, outgoing, true), schema.tables.get(outgoing.toTable).get());
    }

    public List<com.yg.inventory.data.db.Column<Boolean>> buildOrders(Tree<String, View.Node, Join> joinTree,
                                                                      List<OrderBy> orders) {
        return orders.map(o -> buildOrder(joinTree, o));
    }

    com.yg.inventory.data.db.Column<Boolean> buildOrder(Tree<String, View.Node, Join> joinTree, OrderBy order) {
        String alias  = joinTree.getNode(order.path.dropRight(1)).get().alias;
        String column = order.path.last();
        return new com.yg.inventory.data.db.Column<>(alias, column, column, order.assending);
    }

}
