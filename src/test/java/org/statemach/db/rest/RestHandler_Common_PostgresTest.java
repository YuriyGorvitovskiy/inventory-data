package org.statemach.db.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.schema.Schema;
import org.statemach.db.sql.DataAccess;
import org.statemach.db.sql.SQLBuilder;
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
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class RestHandler_Common_PostgresTest {

    final SchemaAccess schemaAccess = new PostgresSchemaAccess(TestDB.jdbc, TestDB.schema);
    final DataAccess   dataAccess   = PostgresDataAccess.of(TestDB.jdbc, TestDB.schema);
    final SQLBuilder   sqlBuilder   = dataAccess.builder();

    final Schema schema = Schema.from(schemaAccess);

    final RestHandler subject = new RestHandler(schema, dataAccess, sqlBuilder);

    final HttpExchange exchange        = mock(HttpExchange.class);
    final HttpContext  context         = mock(HttpContext.class);
    final Headers      responseHeaders = mock(Headers.class);

    final URI    ctxUri  = Java.soft(() -> new URI("http://example.com/rest/"));
    final String ctxPath = "/rest/";

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
        doReturn(context).when(exchange).getHttpContext();
        doReturn(ctxPath).when(context).getPath();
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

    void runTest(String method,
                 String pathAndQuery,
                 String requestResource,
                 int expectCode,
                 String expectedResponse,
                 Object... parameters) {
        // Setup
        String resolvedPath = Java.format(pathAndQuery, parameters);
        URI    uri          = Java.soft(() -> new URI("http://example.com/rest/" + resolvedPath));

        doReturn(method).when(exchange).getRequestMethod();
        doReturn(uri).when(exchange).getRequestURI();

        final String unformattedPayload  = Java.resource(requestResource);
        final String unformattedExpected = Java.resource(expectedResponse);

        final String payload = Java.format(unformattedPayload, parameters);
        final String expect  = Java.format(unformattedExpected, parameters);

        InputStream input = new ByteArrayInputStream(payload.getBytes());
        doReturn(input).when(exchange).getRequestBody();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        doReturn(output).when(exchange).getResponseBody();

        // Setup
        subject.handle(exchange);

        // Verify
        String json         = new String(output.toByteArray());
        var    actualJson   = Json.readAlphabetize(json);
        var    expectedJson = Json.readAlphabetize(expect);

        assertEquals(expectCode, resultCode.get());
        assertEquals(ContentType.APPLICATION_JSON, headers.get(Header.CONTENT_TYPE));
        assertEquals(expectedJson, actualJson);
    }

}
