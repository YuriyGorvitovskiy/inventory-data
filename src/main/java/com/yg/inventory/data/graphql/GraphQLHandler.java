package com.yg.inventory.data.graphql;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.yg.inventory.data.db.DataAccess;
import com.yg.inventory.data.db.SchemaAccess;
import com.yg.util.DB;
import com.yg.util.Json;
import com.yg.util.Rest;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.Scalars;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

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
        GraphQLObjectType queryType = GraphQLObjectType.newObject()
            .name("QueryType")
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name("hello")
                .type(Scalars.GraphQLString))
            .build();

        GraphQLCodeRegistry code = GraphQLCodeRegistry.newCodeRegistry()
            .dataFetcher(FieldCoordinates.coordinates("QueryType", "hello"), (DataFetcher<?>) (e -> fetch("paint", e)))
            .build();

        return GraphQLSchema.newSchema()
            .query(queryType)
            .codeRegistry(code)
            .build();
    }

    /*
    // Make the schema executable
    GraphQL executor = GraphQL.newGraphQL(graphQLSchema).build();
    ExecutionResult executionResult = executor.execute("{hello}");
    
    
    
    SchemaGenerator        schemaGenerator = new SchemaGenerator();
    TypeDefinitionRegistry registry        = new TypeDefinitionRegistry();
    RuntimeWiring          wiring          = buildRuntimeWiring();
    
    GraphQLObjectType type = null;
    registry.add(type);
    return schemaGenerator.makeExecutableSchema(registry, wiring);
    }
    
    private RuntimeWiring buildRuntimeWiring() {
    Map<String, Map<String, DataType>> tables = schema.getTables();
    RuntimeWiring.Builder              wiring = RuntimeWiring.newRuntimeWiring();
    
    wiring.type("QueryType", w -> {
        tables.forEach(t -> w.dataFetcher(t._1, e -> fetch(t._1, e)));
        return w;
    });
    
    tables.map((t) -> {
        t._2
        .map(c -> new Tuple2<>(c._1, DATA_TYPE_TO_SCALAR.get(c._2).getOrNull()))
        .filter(c -> null != c._2)
        .forEach(c -> type.field(GraphQLFieldDefinition.newFieldDefinition()
            .name(c._1)
            .type(c._2)));
    });
    
    GraphQLObjectType.Builder type = GraphQLObjectType.newObject().name(t._1);
    t._2
        .map(c -> new Tuple2<>(c._1, DATA_TYPE_TO_SCALAR.get(c._2).getOrNull()))
        .filter(c -> null != c._2)
        .forEach(c -> type.field(GraphQLFieldDefinition.newFieldDefinition()
            .name(c._1)
            .type(c._2)));
    return type.build();
    }).forEach(t -> schema.additionalType(t));
    
    
    
    .dataFetcher(fieldName, dataFetcher))
    
    
    GraphQLSchema.Builder              schema = GraphQLSchema.newSchema();
    schema.query(GraphQLObjectType.newObject().)
    
    tables.map((t) -> {
        GraphQLObjectType.Builder type = GraphQLObjectType.newObject().name(t._1);
        t._2
            .map(c -> new Tuple2<>(c._1, DATA_TYPE_TO_SCALAR.get(c._2).getOrNull()))
            .filter(c -> null != c._2)
            .forEach(c -> type.field(GraphQLFieldDefinition.newFieldDefinition()
                .name(c._1)
                .type(c._2)));
        return type.build();
    }).forEach(t -> schema.additionalType(t));
    
    schema.query(GraphQLObjectType.)
    return schema.build();
    // TODO Auto-generated method stub
    return null;
            return wiring.build();
    }
    */
    String fetch(String table, DataFetchingEnvironment environment) throws Exception {
        return "world";
    }

}
