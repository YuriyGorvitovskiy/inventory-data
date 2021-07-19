package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.schema.Schema;
import org.statemach.db.sql.DataAccess;
import org.statemach.db.sql.SchemaAccess;
import org.statemach.db.sql.postgres.PostgresDataAccess;
import org.statemach.db.sql.postgres.PostgresSchemaAccess;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.util.Http.ContentType;
import org.statemach.util.Http.Header;
import org.statemach.util.Java;
import org.statemach.util.Json;
import org.statemach.util.Mutable;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import io.vavr.collection.HashMap;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Schema_PostgressTest {
    final DataAccess   dataAccess   = PostgresDataAccess.of(TestDB.jdbc, TestDB.schema);
    final SchemaAccess schemaAccess = new PostgresSchemaAccess(TestDB.jdbc, TestDB.schema);
    final Schema       schema       = Schema.from(schemaAccess);

    final GraphQLHandler subject = GraphQLHandler.build(schema, dataAccess);

    final HttpExchange exchange        = mock(HttpExchange.class);
    final Headers      responseHeaders = mock(Headers.class);

    final ByteArrayOutputStream         output       = new ByteArrayOutputStream();
    final java.util.Map<String, String> headers      = new java.util.HashMap<>();
    final Mutable<Integer>              resultCode   = new Mutable<>(null);
    final Mutable<Long>                 resultLength = new Mutable<>(null);

    @BeforeAll
    static void setup() {
        TestDB.setup();
    }

    @BeforeEach
    void prepare() throws Exception {
        doReturn(output).when(exchange).getResponseBody();
        doReturn(responseHeaders).when(exchange).getResponseHeaders();
        doAnswer((inv) -> {
            resultCode.set(inv.getArgument(0));
            resultLength.set(inv.getArgument(1));
            return null;
        }).when(exchange).sendResponseHeaders(anyInt(), anyLong());
        doAnswer((inv) -> {
            headers.put(inv.getArgument(0), inv.getArgument(1));
            return null;
        }).when(responseHeaders).set(any(), any());
    }

    @Test
    void listTypes() {
        runTest("list-types.gql", "list-types.expect.json");
    }

    @Test
    void __Field_Types() {
        runTest("__Field-type.gql", "__Field-type.expect.json");
    }

    @Test
    void __Type_Types() {
        runTest("__Type-type.gql", "__Type-type.expect.json");
    }

    @Test
    void MutationType_Types() {
        runTest("MutationType-type.gql", "MutationType-type.expect.json");
    }

    @Test
    void QueryType_Types() {
        runTest("QueryType-type.gql", "QueryType-type.expect.json");
    }

    @Test
    void SortingOrder_Types() {
        runTest("SortingOrder-type.gql", "SortingOrder-type.expect.json");
    }

    @Test
    void __type_Types() {
        runTest("__Type-type.gql", "__Type-type.expect.json");
    }

    @Test
    void first_type() {
        runTest("first-type.gql", "first-type.expect.json");
    }

    @Test
    void second_type() {
        runTest("second-type.gql", "second-type.expect.json");
    }

    @Test
    void third_type() {
        runTest("third-type.gql", "third-type.expect.json");
    }

    @Test
    void version_type() {
        runTest("version-type.gql", "version-type.expect.json");
    }

    @Test
    void first_filter_type() {
        runTest("first_filter-type.gql", "first_filter-type.expect.json");
    }

    @Test
    void second_filter_type() {
        runTest("second_filter-type.gql", "second_filter-type.expect.json");
    }

    @Test
    void third_filter_type() {
        runTest("third_filter-type.gql", "third_filter-type.expect.json");
    }

    @Test
    void version_filter_type() {
        runTest("version_filter-type.gql", "version_filter-type.expect.json");
    }

    @Test
    void first_order_type() {
        runTest("first_order-type.gql", "first_order-type.expect.json");
    }

    @Test
    void second_order_type() {
        runTest("second_order-type.gql", "second_order-type.expect.json");
    }

    @Test
    void third_order_type() {
        runTest("third_order-type.gql", "third_order-type.expect.json");
    }

    @Test
    void version_order_type() {
        runTest("version_order-type.gql", "version_order-type.expect.json");
    }

    @Test
    void first_insert_type() {
        runTest("first_insert-type.gql", "first_insert-type.expect.json");
    }

    @Test
    void second_insert_type() {
        runTest("second_insert-type.gql", "second_insert-type.expect.json");
    }

    @Test
    void third_insert_type() {
        runTest("third_insert-type.gql", "third_insert-type.expect.json");
    }

    @Test
    void first_update_type() {
        runTest("first_update-type.gql", "first_update-type.expect.json");
    }

    @Test
    void second_update_type() {
        runTest("second_update-type.gql", "second_update-type.expect.json");
    }

    @Test
    void third_updatetype() {
        runTest("third_update-type.gql", "third_update-type.expect.json");
    }

    void runTest(String queryResource, String expectedResource) {
        // Setup
        final String TYPE_QUERY  = Java.resource(queryResource);
        final String TYPE_EXPECT = Java.resource(expectedResource);

        GraphQLHandler.Input input = new GraphQLHandler.Input();
        input.query = TYPE_QUERY;
        input.operationName = "";
        input.variables = HashMap.empty();

        // Setup
        subject.execute(exchange, input);

        // Verify
        String json         = new String(output.toByteArray());
        var    actualJson   = Json.readAlphabetize(json);
        var    expectedJson = Json.readAlphabetize(TYPE_EXPECT);

        assertEquals(200, resultCode.get());
        assertEquals(ContentType.APPLICATION_JSON, headers.get(Header.CONTENT_TYPE));
        assertEquals(expectedJson, actualJson);
    }
}
