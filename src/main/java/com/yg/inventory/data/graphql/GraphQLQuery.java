package com.yg.inventory.data.graphql;

import java.util.Collections;
import java.util.UUID;

import com.yg.inventory.data.db.DataAccess;
import com.yg.inventory.data.graphql.GraphQLQueryOrder.Sort;
import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;
import com.yg.util.Java;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.SelectedField;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class GraphQLQuery {

    static interface Argument {
        static final String FILTER = "filter";
        static final String LIMIT  = "limit";
        static final String ORDER  = "order";
        static final String SKIP   = "skip";
    }

    static final String QUERY_TYPE = "QueryType";
    static final String ID_COLUMN  = "id";

    final Schema              schema;
    final DataAccess          dataAccess;
    final GraphQLQueryExtract extract;
    final GraphQLQueryFilter  filter;
    final GraphQLQueryOrder   order;

    GraphQLQuery(Schema schema,
                 DataAccess dataAccess,
                 GraphQLQueryExtract extract,
                 GraphQLQueryFilter filter,
                 GraphQLQueryOrder order) {
        this.schema = schema;
        this.dataAccess = dataAccess;
        this.extract = extract;
        this.filter = filter;
        this.order = order;
    }

    public static GraphQLQuery of(Schema schema, GraphQLNaming naming, DataAccess dataAccess) {
        return new GraphQLQuery(schema,
                dataAccess,
                new GraphQLQueryExtract(schema, naming),
                new GraphQLQueryFilter(schema, naming),
                new GraphQLQueryOrder(schema, naming));
    }

    public GraphQLObjectType buildQueryType() {
        return GraphQLObjectType.newObject()
            .name(QUERY_TYPE)
            .fields(schema.tables.keySet()
                .map(t -> extract.buildQueryField(t, t))
                .toJavaList())
            .build();
    }

    public List<GraphQLType> buildAddtionalTypes() {
        return extract.buildAllTypes()
            .appendAll(filter.buildAllTypes())
            .appendAll(order.buildAllTypes());
    }

    public List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildAllFetchers() {
        return schema.tables.values()
            .flatMap(this::buildTableFetchers)
            .appendAll(buildQueryFetchers())
            .toList();
    }

    List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildQueryFetchers() {
        return schema.tables.values()
            .map(this::buildQueryFetcher)
            .toList();
    }

    Tuple2<FieldCoordinates, DataFetcher<?>> buildQueryFetcher(Table table) {
        return new Tuple2<>(
                FieldCoordinates.coordinates(QUERY_TYPE, table.name),
                e -> fetchQuery(table, e));
    }

    List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildTableFetchers(Table table) {
        return buildOutgoingFetchers(table)
            .appendAll(buildIncommingFetchers(table));
    }

    List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildOutgoingFetchers(Table table) {
        return table.outgoing.values()
            .map(this::buildOutgoingFetcher)
            .toList();
    }

    List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildIncommingFetchers(Table table) {
        return table.incoming.values()
            .map(this::buildIncommingFetcher)
            .toList();
    }

    Tuple2<FieldCoordinates, DataFetcher<?>> buildOutgoingFetcher(ForeignKey foreignKey) {
        return new Tuple2<>(
                FieldCoordinates.coordinates(foreignKey.fromTable, foreignKey.fromColumn),
                e -> fetchOutgoing(foreignKey, e));
    }

    Tuple2<FieldCoordinates, DataFetcher<?>> buildIncommingFetcher(ForeignKey foreignKey) {
        return new Tuple2<>(
                FieldCoordinates.coordinates(foreignKey.toTable, foreignKey.name),
                e -> fetchIncoming(foreignKey, e));
    }

    Object fetchQuery(Table table, DataFetchingEnvironment environment) throws Exception {
        DataFetchingFieldSelectionSet       selectionSet = environment.getSelectionSet();
        List<Tuple2<String, DB.Extract<?>>> select       = List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> table.columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append(ID_COLUMN)
            .distinct()
            .map(c -> new Tuple2<>(c, DB.DATA_TYPE_EXTRACT.get(table.columns.get(c).get().type).get()));

        java.util.Map<String, java.util.List<Object>> filter    = environment.getArgument(Argument.FILTER);
        Tuple2<String, DB.Inject>                     condition = buildCondition(
                HashMap.ofAll(Java.ifNull(filter, Collections.emptyMap())).mapValues(List::ofAll),
                table.columns);

        java.util.List<java.util.Map<String, String>> sorting = environment.getArgument(Argument.ORDER);
        List<Tuple2<String, Boolean>>                 order   = buildOrder(
                List.ofAll(Java.ifNull(sorting, Collections.emptyList())).map(HashMap::ofAll),
                table.columns);

        Integer               skip      = Java.ifNull(environment.getArgument(Argument.SKIP), 0);
        Integer               limit     = Java.ifNull(environment.getArgument(Argument.LIMIT), 10);
        Tuple2<Long, Integer> skipLimit = new Tuple2<>(skip.longValue(), limit);

        return dataAccess.queryByCondition(table.name, select, condition, order, skipLimit)
            .map(m -> m.toJavaMap())
            .toJavaList();
    }

    Tuple2<String, DB.Inject> buildCondition(Map<String, List<Object>> filter, Map<String, Column> columns) {
        return DB.andSqlInjects(filter
            .filterKeys(k -> columns.containsKey(k))
            .map(t -> DB.conditionSqlInject(
                    t._1,
                    columns.get(t._1).get().type,
                    DB.DATA_TYPE_INJECT,
                    t._2)));
    }

    List<Tuple2<String, Boolean>> buildOrder(List<Map<String, String>> argument, Map<String, Column> columns) {
        List<Tuple2<String, Boolean>> order = argument
            .flatMap(m -> m
                .filter(t -> columns.containsKey(t._1))
                .mapValues(s -> Sort.ASC.equals(s)));

        return order.isEmpty() ? List.of(new Tuple2<>(ID_COLUMN, true)) : order;
    }

    Object fetchOutgoing(ForeignKey foreignKey, DataFetchingEnvironment environment) throws Exception {
        Table table = schema.tables.get(foreignKey.fromTable).get();

        DataFetchingFieldSelectionSet       selectionSet = environment.getSelectionSet();
        List<Tuple2<String, DB.Extract<?>>> select       = List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> table.columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append(ID_COLUMN)
            .distinct()
            .map(c -> new Tuple2<>(c, DB.DATA_TYPE_EXTRACT.get(table.columns.get(c).get().type).get()));

        java.util.Map<String, Object> value     = environment.getSource();
        Tuple2<String, DB.Inject>     condition = new Tuple2<>(
                foreignKey.toColumn + " = ?",
                DB.Injects.UUID.apply((UUID) value.get(foreignKey.fromColumn)));
        List<Tuple2<String, Boolean>> order     = List.of(new Tuple2<>(ID_COLUMN, true));
        Tuple2<Long, Integer>         skipLimit = new Tuple2<>(0L, 10);

        return dataAccess.queryByCondition(foreignKey.toTable, select, condition, order, skipLimit)
            .map(m -> m.toJavaMap())
            .getOrNull();
    }

    Object fetchIncoming(ForeignKey foreignKey, DataFetchingEnvironment environment) throws Exception {
        Table table = schema.tables.get(foreignKey.fromTable).get();

        DataFetchingFieldSelectionSet       selectionSet = environment.getSelectionSet();
        List<Tuple2<String, DB.Extract<?>>> select       = List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> table.columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append(ID_COLUMN)
            .distinct()
            .map(c -> new Tuple2<>(c, DB.DATA_TYPE_EXTRACT.get(table.columns.get(c).get().type).get()));

        java.util.Map<String, Object> value     = environment.getSource();
        Tuple2<String, DB.Inject>     condition = new Tuple2<>(
                foreignKey.fromColumn + " = ?",
                DB.Injects.UUID.apply((UUID) value.get(foreignKey.toColumn)));
        List<Tuple2<String, Boolean>> order     = List.of(new Tuple2<>(ID_COLUMN, true));
        Tuple2<Long, Integer>         skipLimit = new Tuple2<>(0L, 10);

        return dataAccess.queryByCondition(foreignKey.fromTable, select, condition, order, skipLimit)
            .map(m -> m.toJavaMap())
            .toJavaList();
    }

}
