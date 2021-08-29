package org.statemach.db.rest;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.net.URI;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.statemach.db.schema.Schema;
import org.statemach.db.sql.DataAccess;
import org.statemach.db.sql.SQLBuilder;
import org.statemach.db.sql.postgres.TestSchema;
import org.statemach.util.Http;
import org.statemach.util.Java;
import org.statemach.util.Mutable;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;

public class RestHandler_UnitTest {

    final DataAccess dataAccess = mock(DataAccess.class);
    final SQLBuilder sqlBuilder = mock(SQLBuilder.class);
    final Schema     schema     = TestSchema.SCHEMA;

    final RestHandler subject = spy(new RestHandler(TestSchema.SCHEMA, dataAccess, sqlBuilder));

    final HttpExchange exchange        = mock(HttpExchange.class);
    final HttpContext  context         = mock(HttpContext.class);
    final Headers      responseHeaders = mock(Headers.class);

    final String ctxPath = "/rest";

    final ByteArrayOutputStream         output       = new ByteArrayOutputStream();
    final java.util.Map<String, String> headers      = new java.util.HashMap<>();
    final Mutable<Integer>              resultCode   = new Mutable<>(null);
    final Mutable<Long>                 resultLength = new Mutable<>(null);

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

    @Test
    void get_schema() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/"));
        doNothing().when(subject).getListOfTables(any());
        doReturn(uri).when(exchange).getRequestURI();

        // Execute
        subject.get(exchange);

        // Verify
        verify(subject).getListOfTables(exchange);
    }

    @Test
    void get_schema_2() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest//22"));
        doNothing().when(subject).getListOfTables(any());
        doReturn(uri).when(exchange).getRequestURI();

        // Execute
        subject.get(exchange);

        // Verify
        verify(subject).getListOfTables(exchange);
    }

    @Test
    void get_table() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/first"));
        doNothing().when(subject).queryTable(any(), any());
        doReturn(uri).when(exchange).getRequestURI();

        // Execute
        subject.get(exchange);

        // Verify
        verify(subject).queryTable(exchange, "first");
    }

    @Test
    void get_table_2() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/first/"));
        doNothing().when(subject).queryTable(any(), any());
        doReturn(uri).when(exchange).getRequestURI();

        // Execute
        subject.get(exchange);

        // Verify
        verify(subject).queryTable(exchange, "first");
    }

    @Test
    void get_row() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/first/1"));
        doNothing().when(subject).getRow(any(), any(), any());
        doReturn(uri).when(exchange).getRequestURI();

        // Execute
        subject.get(exchange);

        // Verify
        verify(subject).getRow(exchange, "first", "1");
    }

    @Test
    void merge_empty() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.merge(exchange));
    }

    @Test
    void merge_empty_2() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest//22"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.merge(exchange));
    }

    @Test
    void merge_empty_3() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/first/"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.merge(exchange));
    }

    @Test
    void update_empty() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.update(exchange));
    }

    @Test
    void update_empty_2() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest//22"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.update(exchange));
    }

    @Test
    void update_empty_3() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/first/"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.update(exchange));
    }

    @Test
    void delete_empty() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.delete(exchange));
    }

    @Test
    void delete_empty_2() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest//22"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.delete(exchange));
    }

    @Test
    void delete_empty_3() {
        // Setup
        URI uri = Java.soft(() -> new URI("http://example.com/rest/first/"));
        doReturn(uri).when(exchange).getRequestURI();

        // Execute && Verify
        assertThrows(Http.Error.class, () -> subject.delete(exchange));
    }
}
