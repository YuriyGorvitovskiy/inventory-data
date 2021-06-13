package com.yg.inventory.data.graphql;

import java.util.UUID;

import com.yg.inventory.data.db.Condition;
import com.yg.inventory.data.db.Join;
import com.yg.inventory.data.db.View;
import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;
import com.yg.util.Tree;

import graphql.Scalars;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import io.vavr.control.Option;

public class GraphQLQueryFilter {

    static final Map<DB.DataType, GraphQLScalarType> DATA_TYPE_TO_SCALAR = HashMap.ofEntries(
            new Tuple2<>(DB.DataType.BIGINT, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.BOOLEAN, Scalars.GraphQLBoolean),
            new Tuple2<>(DB.DataType.INTEGER, Scalars.GraphQLInt),
            new Tuple2<>(DB.DataType.UUID, Scalars.GraphQLID),
            new Tuple2<>(DB.DataType.VARCHAR, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.TEXT_SEARCH_VECTOR, Scalars.GraphQLString));

    final Schema        schema;
    final GraphQLNaming naming;

    public GraphQLQueryFilter(Schema schema, GraphQLNaming naming) {
        this.schema = schema;
        this.naming = naming;
    }

    public List<GraphQLType> buildAllTypes() {
        return schema.tables.values()
            .map(this::buildTypeFor)
            .toList();
    }

    GraphQLType buildTypeFor(Table table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getFilterTypeName(table.name))
            .fields(buildScalarFields(table))
            .fields(buildOutgoingFields(table))
            .fields(buildIncomingFields(table))
            .build();
    }

    java.util.List<GraphQLInputObjectField> buildScalarFields(Table table) {
        return table.columns.values()
            .filter(c -> !table.outgoing.containsKey(c.name))
            .map(this::buildScalarField)
            .toJavaList();
    }

    java.util.List<GraphQLInputObjectField> buildOutgoingFields(Table table) {
        return table.outgoing.values()
            .map(this::buildOutgoingField)
            .toJavaList();
    }

    java.util.List<GraphQLInputObjectField> buildIncomingFields(Table table) {
        return table.incoming.values()
            .map(this::buildIncomingField)
            .toJavaList();
    }

    GraphQLInputObjectField buildScalarField(Column column) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(column.name)
            .type(GraphQLList.list(DATA_TYPE_TO_SCALAR.get(column.type).get()))
            .build();
    }

    GraphQLInputObjectField buildOutgoingField(ForeignKey outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(outgoing.fromColumn)
            .type(naming.getFilterTypeRef(outgoing.toTable))
            .build();
    }

    GraphQLInputObjectField buildIncomingField(ForeignKey outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(outgoing.name)
            .type(naming.getFilterTypeRef(outgoing.fromTable))
            .build();
    }

    public List<Filter> parse(Table table, Object argument, Option<Tuple2<String, Set<UUID>>> columnNameWithIds) {
        return parse(List.empty(), false, table, argument)
            .prependAll(columnNameWithIds.map(t -> buildSubQueryFilter(t._1, t._2)));
    }

    @SuppressWarnings("unchecked")
    List<Filter> parse(List<String> path, boolean plural, Table table, Object argument) {
        if (!(argument instanceof java.util.Map)) {
            return List.empty();
        }

        return HashMap.ofAll((java.util.Map<String, Object>) argument)
            .flatMap(t -> parse(path, plural, table, t._1, t._2))
            .toList();
    }

    List<Filter> parse(List<String> path, boolean plural, Table table, String field, Object value) {
        path = path.append(field);
        Option<ForeignKey> incoming = table.incoming.get(field);
        if (incoming.isDefined()) {
            Option<Table> join = schema.tables.get(incoming.get().fromTable);
            if (join.isEmpty()) {
                return List.empty();
            }
            return parse(path.append(field), true, join.get(), value);
        }

        Option<ForeignKey> outgoing = table.outgoing.get(field);
        if (outgoing.isDefined()) {
            Option<Table> join = schema.tables.get(incoming.get().toTable);
            if (join.isEmpty()) {
                return List.empty();
            }
            return parse(path.append(field), plural, join.get(), value);
        }

        Option<Column> column = table.columns.get(field);
        if (column.isDefined()) {
            return List.of(Filter.of(path, plural, column.get().type, List.ofAll((java.util.List<?>) value)));
        }

        return List.empty();
    }

    public Tree<String, Table, ForeignKeyJoin> buildJoins(Table table, List<Filter> filters) {
        Tree<String, Table, ForeignKeyJoin> tree     = Tree.of(table);
        Tuple2<List<Filter>, List<Filter>>  portions = filters.partition(f -> f.acceptNull);

        tree = portions._1.foldLeft(tree,
                (t, f) -> t.putIfMissed(f.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.INNER)));
        tree = portions._2.foldLeft(tree,
                (t, f) -> t.putIfMissed(f.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.LEFT)));

        return tree;
    }

    Tuple2<ForeignKeyJoin, Table> buildJoin(Table parent, String step, Join.Kind join) {
        Option<ForeignKey> incoming = parent.incoming.get(step);
        if (incoming.isDefined()) {
            return new Tuple2<>(new ForeignKeyJoin(join, incoming.get(), false),
                    schema.tables.get(incoming.get().fromTable).get());
        }

        ForeignKey outgoing = parent.outgoing.get(step).get();
        return new Tuple2<>(new ForeignKeyJoin(join, outgoing, true), schema.tables.get(outgoing.toTable).get());
    }

    public Condition buildWhere(Tree<String, View.Node, Join> joinTree, List<Filter> filters) {
        return Condition.and(filters.map(f -> buildCondition(joinTree, f)));
    }

    Condition buildCondition(Tree<String, View.Node, Join> joinTree, Filter filter) {
        String alias = joinTree.getNode(filter.path.dropRight(1)).get().alias;
        return filter.buildCondition(alias);
    }

    Filter buildSubQueryFilter(String column, Set<UUID> ids) {
        return Filter.of(List.of(column), false, DB.DataType.UUID, ids.toList());
    }
}
