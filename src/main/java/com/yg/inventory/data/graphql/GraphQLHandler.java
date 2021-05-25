package com.yg.inventory.data.graphql;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.yg.inventory.data.db.DataAccess;
import com.yg.inventory.data.db.SchemaAccess;
import com.yg.inventory.data.db.SchemaAccess.ForeignKey;
import com.yg.inventory.data.db.SchemaAccess.PrimaryKey;
import com.yg.util.DB;
import com.yg.util.DB.DataType;
import com.yg.util.DB.Inject;
import com.yg.util.Json;
import com.yg.util.Rest;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.Scalars;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.SelectedField;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public class GraphQLHandler implements HttpHandler {
    static interface Param {
        static final String OPERATION_NAME = "operationName";
        static final String QUERY          = "query";
        static final String VARIABLES      = "variables";
    }

    public static class Input {
        public String              query;
        public String              operationName;
        public Map<String, Object> variables;

        public ExecutionInput buildExecutionInput() {
            return ExecutionInput.newExecutionInput(query)
                .operationName(operationName)
                .variables(variables.toJavaMap())
                .build();
        }
    }

    static final String                              QUERY_TYPE          = "QueryType";
    static final String                              MUTATION_TYPE       = "MutationType";
    static final String                              ID                  = "id";
    static final Map<DB.DataType, GraphQLScalarType> DATA_TYPE_TO_SCALAR = HashMap.ofEntries(
            new Tuple2<>(DB.DataType.BIGINT, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.BOOLEAN, Scalars.GraphQLBoolean),
            new Tuple2<>(DB.DataType.INTEGER, Scalars.GraphQLInt),
            new Tuple2<>(DB.DataType.UUID, Scalars.GraphQLID),
            new Tuple2<>(DB.DataType.VARCHAR, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.TEXT_SEARCH_VECTOR, Scalars.GraphQLString));

    final SchemaAccess schema  = new SchemaAccess();
    final DataAccess   data    = new DataAccess();
    final GraphQL      graphQL = GraphQL.newGraphQL(generateSchema()).build();

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        switch (exchange.getRequestMethod().toUpperCase()) {
            case "GET":
                get(exchange);
                break;
            case "POST":
                post(exchange);
                break;
            default:
                break;
        }
    }

    @SuppressWarnings("unchecked")
    void get(HttpExchange exchange) {
        Map<String, List<String>> params = Rest.queryParams(exchange);
        Input                     input  = new Input();

        input.query = params.get(Param.QUERY)
            .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.BAD_REQUEST,
                    "HTTP GET request should have request parameter '${0}' specified",
                    Param.QUERY))
            .get();

        String variablesJson = params.get(Param.VARIABLES).map(l -> l.get()).getOrNull();
        input.variables = Json.parse(variablesJson, Map.class);
        input.operationName = params.get(Param.OPERATION_NAME).map(l -> l.get()).getOrNull();

        execute(exchange, input);
    }

    void post(HttpExchange exchange) {
        Input input = Rest.extract(exchange, Input.class);

        execute(exchange, input);
    }

    void execute(HttpExchange exchange, Input input) {
        ExecutionResult result = graphQL.execute(input.buildExecutionInput());
        Rest.json(exchange, result.toSpecification());
    }

    GraphQLSchema generateSchema() {
        Map<String, Map<String, DataType>> tables = schema.getTables();

        GraphQLObjectType queryType = GraphQLObjectType.newObject().name(QUERY_TYPE)
            .fields(tables.keySet()
                .map(t -> GraphQLFieldDefinition.newFieldDefinition()
                    .name(t)
                    .type(GraphQLList.list(new GraphQLTypeReference(t)))
                    .argument(GraphQLArgument.newArgument()
                        .name("filter")
                        .type(new GraphQLTypeReference(t + "_filter")))
                    .argument(GraphQLArgument.newArgument()
                        .name("order")
                        .type(GraphQLList.list(new GraphQLTypeReference(t + "_order"))))
                    .argument(GraphQLArgument.newArgument()
                        .name("skip")
                        .type(Scalars.GraphQLInt))
                    .argument(GraphQLArgument.newArgument()
                        .name("limit")
                        .type(Scalars.GraphQLInt))
                    .build())
                .toJavaList())
            .build();

        GraphQLObjectType mutationType = GraphQLObjectType.newObject().name(MUTATION_TYPE)
            .fields(tables.keySet()
                .flatMap(t -> generateMutations(t))
                .toJavaList())
            .build();

        Map<String, PrimaryKey> primaryKeys = schema.getAllPrimaryKeys()
            .toMap(p -> new Tuple2<>(p.table, p));

        List<ForeignKey>                     foreignKeys = schema.getAllForeignKeys();
        Map<String, Map<String, ForeignKey>> outcoming   = foreignKeys
            .groupBy(f -> f.fromTable)
            .mapValues(l -> l.toMap(f -> new Tuple2<>(f.fromColumn, f)));
        Map<String, Map<String, ForeignKey>> incoming    = foreignKeys
            .groupBy(f -> f.toTable)
            .mapValues(l -> l.toMap(f -> new Tuple2<>(f.name, f)));

        List<Tuple3<GraphQLType, String, Map<String, DataFetcher<?>>>> queryTypes = tables
            .map(t -> generateQueryType(t._1,
                    t._2,
                    primaryKeys.get(t._1),
                    outcoming.getOrElse(t._1, HashMap.empty()),
                    incoming.getOrElse(t._1, HashMap.empty())))
            .toList();

        List<GraphQLType> queryInputTypes = tables
            .flatMap(
                    t -> generateQueryInputTypes(t._1, t._2, primaryKeys.get(t._1), outcoming.getOrElse(t._1, HashMap.empty())))
            .toList();

        List<GraphQLType> mutationTypes = tables
            .flatMap(t -> generateMutationTypes(t._1, t._2, primaryKeys.get(t._1), outcoming.getOrElse(t._1, HashMap.empty())))
            .toList();

        GraphQLCodeRegistry.Builder code = GraphQLCodeRegistry.newCodeRegistry();
        code.dataFetchers(QUERY_TYPE,
                tables.keySet()
                    .map(t -> new Tuple2<>(t, (DataFetcher<?>) (e -> fetchQuery(t, e))))
                    .toJavaMap(t -> t));
        code.dataFetchers(MUTATION_TYPE,
                tables.keySet()
                    .flatMap(t -> List.of(
                            new Tuple2<>("insert_" + t, (DataFetcher<?>) (e -> fetchInsert(t, tables.get(t).get(), e))),
                            new Tuple2<>("upsert_" + t, (DataFetcher<?>) (e -> fetchUpsert(t, tables.get(t).get(), e))),
                            new Tuple2<>("update_" + t, (DataFetcher<?>) (e -> fetchUpdate(t, tables.get(t).get(), e))),
                            new Tuple2<>("delete_" + t, (DataFetcher<?>) (e -> fetchDelete(t, tables.get(t).get(), e)))))
                    .toJavaMap(t -> t));

        queryTypes.forEach(t -> code.dataFetchers(t._2, t._3.toJavaMap()));
        GraphQLEnumType sortingOrder = GraphQLEnumType.newEnum()
            .name("SortingOrder")
            .value("ASC")
            .value("DESC")
            .build();
        return GraphQLSchema.newSchema()
            .query(queryType)
            .mutation(mutationType)
            .additionalTypes(queryTypes.map(t -> t._1).toJavaSet())
            .additionalTypes(queryInputTypes.toJavaSet())
            .additionalTypes(mutationTypes.toJavaSet())
            .additionalType(sortingOrder)
            .codeRegistry(code.build())
            .build();
    }

    Tuple3<GraphQLType, String, Map<String, DataFetcher<?>>> generateQueryType(String table,
                                                                               Map<String, DataType> columns,
                                                                               Option<PrimaryKey> pk,
                                                                               Map<String, ForeignKey> out,
                                                                               Map<String, ForeignKey> in) {
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name(table);
        builder.fields(columns
            .filterKeys(c -> !out.containsKey(c))
            .filterValues(d -> DB.DataType.TEXT_SEARCH_VECTOR != d)
            .map(t -> generateField(
                    t._1,
                    t._2,
                    isReference(t._1, pk, out)))
            .toJavaList());

        builder.fields(out.values()
            .map(fk -> GraphQLFieldDefinition.newFieldDefinition()
                .name(fk.fromColumn)
                .type(new GraphQLTypeReference(fk.toTable))
                .build())
            .toJavaList());

        builder.fields(in.values()
            .map(fk -> GraphQLFieldDefinition.newFieldDefinition()
                .name(fk.name)
                .type(GraphQLList.list(new GraphQLTypeReference(fk.fromTable)))
                .build())
            .toJavaList());

        Map<String, DataFetcher<?>> outDataFetchers = out.values()
            .map(fk -> new Tuple2<>(fk.fromColumn, (DataFetcher<?>) (e -> fetchOut(fk, e))))
            .toMap(t -> t);

        Map<String, DataFetcher<?>> inDataFetchers = in.values()
            .map(fk -> new Tuple2<>(fk.name, (DataFetcher<?>) (e -> fetchIn(fk, e))))
            .toMap(t -> t);

        return new Tuple3<>(builder.build(), table, outDataFetchers.merge(inDataFetchers));
    }

    GraphQLFieldDefinition generateField(String name, DataType type, boolean isReference) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(name)
            .type(isReference ? Scalars.GraphQLID : DATA_TYPE_TO_SCALAR.get(type).get())
            .build();
    }

    GraphQLInputObjectField generateInputField(String name, DataType type, boolean isReference) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(name)
            .type(isReference ? Scalars.GraphQLID : DATA_TYPE_TO_SCALAR.get(type).get())
            .build();
    }

    List<GraphQLFieldDefinition> generateMutations(String table) {
        return List.of(
                generateInsertMutations(table),
                generateUpsertMutations(table),
                generateUpdateMutations(table),
                generateDeleteMutations(table));
    }

    GraphQLFieldDefinition generateInsertMutations(String table) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name("insert_" + table)
            .type(new GraphQLTypeReference(table))
            .argument(GraphQLArgument.newArgument()
                .name(table)
                .type(new GraphQLTypeReference(table + "_insert")))
            .build();
    }

    GraphQLFieldDefinition generateUpsertMutations(String table) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name("upsert_" + table)
            .type(new GraphQLTypeReference(table))
            .argument(GraphQLArgument.newArgument()
                .name(ID)
                .type(Scalars.GraphQLID))
            .argument(GraphQLArgument.newArgument()
                .name(table)
                .type(new GraphQLTypeReference(table + "_update")))
            .build();
    }

    GraphQLFieldDefinition generateUpdateMutations(String table) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name("update_" + table)
            .type(new GraphQLTypeReference(table))
            .argument(GraphQLArgument.newArgument()
                .name(ID)
                .type(Scalars.GraphQLID))
            .argument(GraphQLArgument.newArgument()
                .name(table)
                .type(new GraphQLTypeReference(table + "_update")))
            .build();
    }

    GraphQLFieldDefinition generateDeleteMutations(String table) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name("delete_" + table)
            .type(new GraphQLTypeReference(table))
            .argument(GraphQLArgument.newArgument()
                .name(ID)
                .type(Scalars.GraphQLID))
            .build();
    }

    List<GraphQLType> generateQueryInputTypes(String table,
                                              Map<String, DataType> columns,
                                              Option<PrimaryKey> pk,
                                              Map<String, ForeignKey> out) {
        GraphQLInputObjectType filter = GraphQLInputObjectType.newInputObject()
            .name(table + "_filter")
            .fields(columns
                .map(t -> generateFilterField(t._1, t._2, isReference(t._1, pk, out)))
                .toJavaList())
            .build();

        GraphQLInputObjectType order = GraphQLInputObjectType.newInputObject()
            .name(table + "_order")
            .fields(columns
                .filterValues(d -> DB.DataType.TEXT_SEARCH_VECTOR != d)
                .map(t -> GraphQLInputObjectField.newInputObjectField()
                    .name(t._1)
                    .type(GraphQLTypeReference.typeRef("SortingOrder"))
                    .build())
                .toJavaList())
            .build();

        return List.of(filter, order);
    }

    boolean isReference(String column, Option<PrimaryKey> pk, Map<String, ForeignKey> out) {
        return column.equals(pk.map(p -> p.column).getOrNull()) || out.containsKey(column);
    }

    GraphQLInputObjectField generateFilterField(String name, DataType type, boolean isReference) {
        GraphQLType primitive = isReference ? Scalars.GraphQLID : DATA_TYPE_TO_SCALAR.get(type).get();

        return GraphQLInputObjectField.newInputObjectField()
            .name(name)
            .type(GraphQLList.list(primitive))
            .build();
    }

    List<GraphQLType> generateMutationTypes(String table,
                                            Map<String, DataType> columns,
                                            Option<PrimaryKey> pk,
                                            Map<String, ForeignKey> out) {
        GraphQLInputObjectType insert = GraphQLInputObjectType.newInputObject()
            .name(table + "_insert")
            .fields(columns
                .filterValues(d -> DB.DataType.TEXT_SEARCH_VECTOR != d)
                .map(t -> generateInputField(t._1,
                        t._2,
                        t._1.equals(pk.map(p -> p.column).getOrNull()) || out.containsKey(t._1)))
                .toJavaList())
            .build();

        GraphQLInputObjectType update = GraphQLInputObjectType.newInputObject()
            .name(table + "_update")
            .fields(columns
                .filterKeys(c -> !c.equals(pk.map(p -> p.column).getOrNull()))
                .filterValues(d -> DB.DataType.TEXT_SEARCH_VECTOR != d)
                .map(t -> generateInputField(t._1, t._2, out.containsKey(t._1)))
                .toJavaList())
            .build();

        return List.of(insert, update);
    }

    java.util.List<java.util.Map<String, Object>> fetchQuery(String table,
                                                             DataFetchingEnvironment environment) throws Exception {
        Map<String, DB.DataType> columns = schema.getTableColumns(table);

        DataFetchingFieldSelectionSet       selectionSet = environment.getSelectionSet();
        List<Tuple2<String, DB.Extract<?>>> select       = List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append(ID)
            .distinct()
            .map(c -> new Tuple2<>(c, DB.DATA_TYPE_EXTRACT.get(columns.get(c).get()).get()));

        Tuple2<String, DB.Inject>     condition = new Tuple2<>("1 = 1", DB.Injects.NOTHING);
        List<Tuple2<String, Boolean>> order     = List.of(new Tuple2<>(columns.get()._1, true));
        Tuple2<Long, Integer>         skipLimit = new Tuple2<>(0L, 10);

        return data.queryByCondition(table, select, condition, order, skipLimit)
            .map(m -> m.toJavaMap())
            .toJavaList();
    }

    Object fetchOut(ForeignKey fk, DataFetchingEnvironment environment) throws Exception {
        Map<String, DB.DataType> columns = schema.getTableColumns(fk.toTable);

        DataFetchingFieldSelectionSet       selectionSet = environment.getSelectionSet();
        List<Tuple2<String, DB.Extract<?>>> select       = List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append(ID)
            .distinct()
            .map(c -> new Tuple2<>(c, DB.DATA_TYPE_EXTRACT.get(columns.get(c).get()).get()));

        java.util.Map<String, Object> value     = environment.getSource();
        Tuple2<String, DB.Inject>     condition = new Tuple2<>(
                fk.toColumn + " = ?",
                DB.Injects.UUID.apply((UUID) value.get(fk.fromColumn)));
        List<Tuple2<String, Boolean>> order     = List.of(new Tuple2<>(columns.get()._1, true));
        Tuple2<Long, Integer>         skipLimit = new Tuple2<>(0L, 10);

        return data.queryByCondition(fk.toTable, select, condition, order, skipLimit)
            .map(m -> m.toJavaMap())
            .getOrElse(Collections.emptyMap());
    }

    Object fetchIn(ForeignKey fk, DataFetchingEnvironment environment) throws Exception {
        Map<String, DB.DataType> columns = schema.getTableColumns(fk.fromTable);

        DataFetchingFieldSelectionSet       selectionSet = environment.getSelectionSet();
        List<Tuple2<String, DB.Extract<?>>> select       = List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append(ID)
            .distinct()
            .map(c -> new Tuple2<>(c, DB.DATA_TYPE_EXTRACT.get(columns.get(c).get()).get()));

        java.util.Map<String, Object> value     = environment.getSource();
        Tuple2<String, DB.Inject>     condition = new Tuple2<>(
                fk.fromColumn + " = ?",
                DB.Injects.UUID.apply((UUID) value.get(fk.toColumn)));
        List<Tuple2<String, Boolean>> order     = List.of(new Tuple2<>(columns.get()._1, true));
        Tuple2<Long, Integer>         skipLimit = new Tuple2<>(0L, 10);

        return data.queryByCondition(fk.fromTable, select, condition, order, skipLimit)
            .map(m -> m.toJavaMap())
            .toJavaList();
    }

    java.util.Map<String, Object> fetchInsert(String table,
                                              Map<String, DataType> columns,
                                              DataFetchingEnvironment environment) throws Exception {
        java.util.Map<String, Object> entity = environment.getArgument(table);
        List<Tuple2<String, Inject>>  insert = HashMap.ofAll(entity).flatMap(t -> getInject(columns, t._1, t._2)).toList();

        return data.insert(table, insert, returnFields(columns, environment)).get().toJavaMap();
    }

    java.util.Map<String, Object> fetchUpsert(String table,
                                              Map<String, DataType> columns,
                                              DataFetchingEnvironment environment) throws Exception {
        UUID                          id     = UUID.fromString(environment.getArgument(ID));
        java.util.Map<String, Object> entity = environment.getArgument(table);
        List<Tuple2<String, Inject>>  upsert = HashMap.ofAll(entity).flatMap(t -> getInject(columns, t._1, t._2)).toList();

        return data.mergeById(table, id, upsert, returnFields(columns, environment)).get().toJavaMap();
    }

    java.util.Map<String, Object> fetchUpdate(String table,
                                              Map<String, DataType> columns,
                                              DataFetchingEnvironment environment) throws Exception {
        UUID                          id     = UUID.fromString(environment.getArgument(ID));
        java.util.Map<String, Object> entity = environment.getArgument(table);
        List<Tuple2<String, Inject>>  upsert = HashMap.ofAll(entity).flatMap(t -> getInject(columns, t._1, t._2)).toList();

        return data.updateById(table, id, upsert, returnFields(columns, environment)).get().toJavaMap();
    }

    java.util.Map<String, Object> fetchDelete(String table,
                                              Map<String, DataType> columns,
                                              DataFetchingEnvironment environment) throws Exception {
        UUID id = UUID.fromString(environment.getArgument(ID));
        return data.deleteById(table, id, returnFields(columns, environment)).get().toJavaMap();
    }

    Option<Tuple2<String, Inject>> getInject(Map<String, DataType> columns, String column, Object value) {
        Option<DataType> dt = columns.get(column);
        if (dt.isEmpty()) {
            return Option.none();
        }

        Option<Function<Object, Inject>> injector = DB.DATA_TYPE_INJECT.get(dt.get());
        if (dt.isEmpty()) {
            return Option.none();
        }

        return Option.of(new Tuple2<>(column, injector.get().apply(value)));
    }

    List<Tuple2<String, DB.Extract<?>>> returnFields(Map<String, DataType> columns, DataFetchingEnvironment environment) {
        DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();
        return List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append(ID)
            .distinct()
            .map(c -> new Tuple2<>(c, DB.DATA_TYPE_EXTRACT.get(columns.get(c).get()).get()));
    }

}
