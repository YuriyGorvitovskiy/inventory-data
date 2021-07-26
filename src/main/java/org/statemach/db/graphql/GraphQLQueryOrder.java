package org.statemach.db.graphql;

import java.util.Collection;
import java.util.Objects;

import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.Schema;
import org.statemach.db.schema.TableInfo;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.Select;
import org.statemach.db.sql.postgres.PostgresDataType;
import org.statemach.util.NodeLinkTree;

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

    GraphQLType buildType(TableInfo table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getOrderTypeName(table.name))
            .fields(buildScalarFields(table))
            .fields(buildOutgoingFields(table))
            .build();
    }

    GraphQLType buildSortingOrderEnum() {
        return GraphQLEnumType.newEnum()
            .name(SORTING_ORDER_TYPE_NAME)
            .value(Sort.ASC)
            .value(Sort.DESC)
            .build();
    }

    java.util.List<GraphQLInputObjectField> buildScalarFields(TableInfo table) {
        return table.columns.values()
            .filter(this::isOrderableColumn)
            .map(c -> buildScalarField(c, table.outgoing.get(c.name)))
            .toJavaList();
    }

    java.util.List<GraphQLInputObjectField> buildOutgoingFields(TableInfo table) {
        return table.outgoing.values()
            .map(this::buildOutgoingField)
            .toJavaList();
    }

    boolean isOrderableColumn(ColumnInfo column) {
        return PostgresDataType.TSVECTOR != column.type;
    }

    GraphQLInputObjectField buildScalarField(ColumnInfo column, Option<ForeignKey> outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(column.name)
            .type(outgoing.isDefined()
                    ? naming.getOrderTypeRef(outgoing.get().toTable)
                    : SORTING_ORDER_TYPE_REF)
            .build();
    }

    GraphQLInputObjectField buildOutgoingField(ForeignKey outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(outgoing.name)
            .type(naming.getOrderTypeRef(outgoing.toTable))
            .build();
    }

    public List<OrderBy> parse(TableInfo table, Object argument) {
        if (!(argument instanceof Collection)) {
            return List.empty();
        }
        return List.ofAll((Collection<?>) argument)
            .flatMap(a -> parse(List.empty(), table, a));
    }

    @SuppressWarnings("unchecked")
    List<OrderBy> parse(List<String> path, TableInfo table, Object argument) {
        if (!(argument instanceof java.util.Map)) {
            return List.empty();
        }

        return HashMap.ofAll((java.util.Map<String, Object>) argument)
            .flatMap(t -> parse(path, table, t._1, t._2))
            .toList();
    }

    List<OrderBy> parse(List<String> path, TableInfo table, String field, Object value) {
        Option<ForeignKey> outgoing = table.outgoing.get(field);
        if (outgoing.isDefined()) {
            Option<TableInfo> join = schema.tables.get(outgoing.get().toTable);
            if (join.isEmpty()) {
                return List.empty();
            }
            return parse(path.append(field), join.get(), value);
        }

        Option<ColumnInfo> column = table.columns.get(field);
        if (column.isDefined()) {
            return List.of(new OrderBy(path.append(field), !Sort.DESC.equalsIgnoreCase(Objects.toString(value))));
        }
        return List.empty();
    }

    public NodeLinkTree<String, TableInfo, ForeignKeyJoin> buildJoins(NodeLinkTree<String, TableInfo, ForeignKeyJoin> tree,
                                                                      List<OrderBy> sorting) {
        return sorting.foldLeft(tree,
                (t, s) -> t.putIfMissed(s.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.LEFT)));
    }

    Tuple2<ForeignKeyJoin, TableInfo> buildJoin(TableInfo parent, String step, Join.Kind join) {
        ForeignKey outgoing = parent.outgoing.get(step).get();
        return new Tuple2<>(new ForeignKeyJoin(join, outgoing, true), schema.tables.get(outgoing.toTable).get());
    }

    public List<Select<Boolean>> buildOrders(NodeLinkTree<String, From, Join> joinTree, List<OrderBy> orders) {
        return orders.map(o -> buildOrder(joinTree, o));
    }

    Select<Boolean> buildOrder(NodeLinkTree<String, From, Join> joinTree, OrderBy order) {
        String alias  = joinTree.getNode(order.path.dropRight(1)).get().alias;
        String column = order.path.last();
        return Select.of(alias, column, order.assending);
    }

}
