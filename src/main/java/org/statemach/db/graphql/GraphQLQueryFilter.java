package org.statemach.db.graphql;

import java.util.Collections;

import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.Schema;
import org.statemach.db.schema.TableInfo;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.SQLBuilder;
import org.statemach.util.NodeLinkTree;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLType;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.control.Option;

public class GraphQLQueryFilter {

    final Schema         schema;
    final SQLBuilder     sql;
    final GraphQLNaming  naming;
    final GraphQLMapping mapping;

    public GraphQLQueryFilter(Schema schema, SQLBuilder sql, GraphQLNaming naming, GraphQLMapping mapping) {
        this.schema = schema;
        this.sql = sql;
        this.naming = naming;
        this.mapping = mapping;
    }

    public List<GraphQLType> buildAllTypes() {
        return schema.tables.values()
            .map(this::buildTypeFor)
            .toList();
    }

    GraphQLType buildTypeFor(TableInfo table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getFilterTypeName(table.name))
            .fields(buildScalarFields(table))
            .fields(buildOutgoingFields(table))
            .fields(buildIncomingFields(table))
            .build();
    }

    java.util.List<GraphQLInputObjectField> buildScalarFields(TableInfo table) {
        return table.columns.values()
            .filter(c -> mapping.isFilterable(c.type))
            .map(c -> buildScalarField(table, c))
            .toJavaList();
    }

    java.util.List<GraphQLInputObjectField> buildOutgoingFields(TableInfo table) {
        return table.outgoing.values()
            .map(this::buildOutgoingField)
            .toJavaList();
    }

    java.util.List<GraphQLInputObjectField> buildIncomingFields(TableInfo table) {
        return table.incoming.values()
            .map(this::buildIncomingField)
            .toJavaList();
    }

    GraphQLInputObjectField buildScalarField(TableInfo table, ColumnInfo column) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(column.name)
            .type(GraphQLList.list(mapping.scalar(table, column)))
            .build();
    }

    GraphQLInputObjectField buildOutgoingField(ForeignKey outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(outgoing.name)
            .type(naming.getFilterTypeRef(outgoing.toTable))
            .build();
    }

    GraphQLInputObjectField buildIncomingField(ForeignKey incoming) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(naming.getReverseName(incoming.name))
            .type(naming.getFilterTypeRef(incoming.fromTable))
            .build();
    }

    public List<Filter> parse(TableInfo table, Object argument) {
        return parse(List.empty(), false, table, argument);
    }

    @SuppressWarnings("unchecked")
    List<Filter> parse(List<String> path, boolean plural, TableInfo table, Object argument) {
        if (!(argument instanceof java.util.Map)) {
            return List.empty();
        }

        return HashMap.ofAll((java.util.Map<String, Object>) argument)
            .flatMap(t -> parse(path, plural, table, t._1, t._2))
            .toList();
    }

    List<Filter> parse(List<String> path, boolean plural, TableInfo table, String field, Object value) {
        path = path.append(field);
        Option<ColumnInfo> column = table.columns.get(field);
        if (column.isDefined()) {
            java.util.List<?> valuesAsList = value instanceof java.util.List<?>
                    ? (java.util.List<?>) value
                    : Collections.singletonList(value);
            return List.of(Filter.of(path, plural, column.get().type, valuesAsList));
        }

        Option<ForeignKey> outgoing = table.outgoing.get(field);
        if (outgoing.isDefined()) {
            TableInfo join = schema.tables.get(outgoing.get().toTable).get();
            return parse(path, plural, join, value);
        }

        Option<ForeignKey> incoming = table.incoming.get(naming.getForeignKey(field));
        TableInfo          join     = schema.tables.get(incoming.get().fromTable).get();
        return parse(path, true, join, value);
    }

    public NodeLinkTree<String, TableInfo, ForeignKeyJoin> buildJoins(TableInfo table, List<Filter> filters) {
        NodeLinkTree<String, TableInfo, ForeignKeyJoin> tree     = NodeLinkTree.of(table);
        Tuple2<List<Filter>, List<Filter>>              portions = filters.partition(f -> f.acceptNull);

        tree = portions._1.foldLeft(tree,
                (t, f) -> t.putIfMissed(f.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.INNER)));
        tree = portions._2.foldLeft(tree,
                (t, f) -> t.putIfMissed(f.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.LEFT)));

        return tree;
    }

    Tuple2<ForeignKeyJoin, TableInfo> buildJoin(TableInfo parent, String step, Join.Kind join) {
        Option<ForeignKey> outgoing = parent.outgoing.get(step);
        if (outgoing.isDefined()) {
            return new Tuple2<>(new ForeignKeyJoin(join, outgoing.get(), true),
                    schema.tables.get(outgoing.get().toTable).get());
        }

        Option<ForeignKey> incoming = parent.incoming.get(naming.getForeignKey(step));
        return new Tuple2<>(new ForeignKeyJoin(join, incoming.get(), false),
                schema.tables.get(incoming.get().fromTable).get());
    }

    public Condition buildWhere(NodeLinkTree<String, From, Join> joinTree, List<Filter> filters) {
        return sql.and(filters.map(f -> buildCondition(joinTree, f)));
    }

    Condition buildCondition(NodeLinkTree<String, From, Join> joinTree, Filter filter) {
        String alias = joinTree.getNode(filter.path.dropRight(1)).get().alias;
        return filter.buildCondition(mapping, sql, alias);
    }

}
