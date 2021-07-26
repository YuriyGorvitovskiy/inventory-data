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

public class GraphQLHandler_Common_PostgresTest {
    final SchemaAccess schemaAccess = new PostgresSchemaAccess(TestDB.jdbc, TestDB.schema);
    final DataAccess   dataAccess   = PostgresDataAccess.of(TestDB.jdbc, TestDB.schema);
    final Schema       schema       = Schema.from(schemaAccess);

    final GraphQLHandler subject = GraphQLHandler.build(schema, schemaAccess, dataAccess);

    final HttpExchange exchange        = mock(HttpExchange.class);
    final Headers      responseHeaders = mock(Headers.class);

    final ByteArrayOutputStream         output       = new ByteArrayOutputStream();
    final java.util.Map<String, String> headers      = new java.util.HashMap<>();
    final Mutable<Integer>              resultCode   = new Mutable<>(null);
    final Mutable<Long>                 resultLength = new Mutable<>(null);

    @BeforeAll
    static void setup() {
        TestDB.setup();
        TestDB.truncateAll();
        TestDB.insertAll();
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

    void runTest(String queryResource, String expectedResource, Object... parameters) {
        // Setup
        final String TYPE_QUERY  = Java.resource(queryResource);
        final String TYPE_EXPECT = Java.resource(expectedResource);

        GraphQLHandler.Input input = new GraphQLHandler.Input();
        input.query = Java.format(TYPE_QUERY, parameters);
        input.operationName = "";
        input.variables = HashMap.empty();

        // Setup
        subject.execute(exchange, input);

        // Verify
        String json         = new String(output.toByteArray());
        var    actualJson   = Json.readAlphabetize(json);
        var    expectedJson = Json.readAlphabetize(Java.format(TYPE_EXPECT, parameters));

        assertEquals(200, resultCode.get());
        assertEquals(ContentType.APPLICATION_JSON, headers.get(Header.CONTENT_TYPE));
        assertEquals(expectedJson, actualJson);
    }

}
