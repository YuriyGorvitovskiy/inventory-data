package com.yg.inventory.data.graphql;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.yg.inventory.data.db.DataAccess;
import com.yg.inventory.data.db.SchemaAccess;
import com.yg.inventory.data.db.SchemaAccess.ForeignKey;
import com.yg.inventory.data.db.SchemaAccess.PrimaryKey;
import com.yg.util.DB;
import com.yg.util.DB.DataType;
import com.yg.util.Json;
import com.yg.util.Rest;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.Scalars;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
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
    static final Map<DB.DataType, GraphQLScalarType> DATA_TYPE_TO_SCALAR = HashMap.ofEntries(
            new Tuple2<>(DB.DataType.BIGINT, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.BOOLEAN, Scalars.GraphQLBoolean),
            new Tuple2<>(DB.DataType.INTEGER, Scalars.GraphQLInt),
            new Tuple2<>(DB.DataType.UUID, Scalars.GraphQLID),
            new Tuple2<>(DB.DataType.VARCHAR, Scalars.GraphQLString));

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
                    .type(new GraphQLList(new GraphQLTypeReference(t)))
                    .build())
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

        List<Tuple3<GraphQLType, String, Map<String, DataFetcher<?>>>> additionalTypes = tables
            .map(t -> generateType(t._1,
                    t._2,
                    primaryKeys.get(t._1),
                    outcoming.getOrElse(t._1, HashMap.empty()),
                    incoming.getOrElse(t._1, HashMap.empty())))
            .toList();

        GraphQLCodeRegistry.Builder code = GraphQLCodeRegistry.newCodeRegistry();
        code.dataFetchers(QUERY_TYPE,
                tables.keySet()
                    .map(t -> new Tuple2<>(t, (DataFetcher<?>) (e -> fetchQuery(t, e))))
                    .toJavaMap(t -> t));

        additionalTypes.forEach(t -> code.dataFetchers(t._2, t._3.toJavaMap()));

        return GraphQLSchema.newSchema()
            .query(queryType)
            .additionalTypes(additionalTypes.map(t -> t._1).toJavaSet())
            .codeRegistry(code.build())
            .build();
    }

    Tuple3<GraphQLType, String, Map<String, DataFetcher<?>>> generateType(String table,
                                                                          Map<String, DataType> columns,
                                                                          Option<PrimaryKey> pk,
                                                                          Map<String, ForeignKey> out,
                                                                          Map<String, ForeignKey> in) {
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name(table);
        builder.fields(columns
            .filterValues(d -> DB.DataType.TEXT_SEARCH_VECTOR != d)
            .map(t -> generateFiled(
                    t._1,
                    t._2,
                    t._1.equals(pk.map(p -> p.column).getOrNull()) || out.containsKey(t._1)))
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
                .type(new GraphQLList(new GraphQLTypeReference(fk.fromTable)))
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

    GraphQLFieldDefinition generateFiled(String name, DataType type, boolean isReference) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(name)
            .type(isReference ? Scalars.GraphQLID : DATA_TYPE_TO_SCALAR.get(type).get())
            .build();
    }

    java.util.List<java.util.Map<String, Object>> fetchQuery(String table,
                                                             DataFetchingEnvironment environment) throws Exception {
        Map<String, DB.DataType> columns = schema.getTableColumns(table);

        DataFetchingFieldSelectionSet       selectionSet = environment.getSelectionSet();
        List<Tuple2<String, DB.Extract<?>>> select       = List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append("id")
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
            .append("id")
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
            .append("id")
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
}
