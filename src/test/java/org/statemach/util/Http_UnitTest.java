package org.statemach.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;

import org.junit.jupiter.api.Test;
import org.statemach.util.Http.ContentType;
import org.statemach.util.Http.Error;
import org.statemach.util.Http.ErrorCode;
import org.statemach.util.Http.ErrorResource;
import org.statemach.util.Http.Header;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.LinkedHashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class Http_UnitTest {

    @Test
    void Error() {
        // Execute
        final Error result = new Error(ErrorCode.BAD_REQUEST, "Test ${0} message", 22);

        // Verify
        assertEquals(ErrorCode.BAD_REQUEST, result.errorCode);
        assertEquals("Test 22 message", result.getMessage());
        assertNull(result.getCause());
    }

    @Test
    void Error_throwable() {
        final Exception ex = new Exception();
        // Execute
        final Error result = new Error(ErrorCode.UNAUTHORIZED, ex);

        // Verify
        assertEquals(ErrorCode.UNAUTHORIZED, result.errorCode);
        assertEquals("java.lang.Exception", result.getMessage());
        assertEquals(ex, result.getCause());
    }

    @Test
    void Error_throwable_message() {
        // Setup
        final Exception ex = new Exception();

        // Execute
        final Error result = new Error(ErrorCode.NOT_FOUND, ex, "Test ${0} message", 22);

        // Verify
        assertEquals(ErrorCode.NOT_FOUND, result.errorCode);
        assertEquals("Test 22 message", result.getMessage());
        assertEquals(ex, result.getCause());
    }

    @Test
    void ErrorResource() {
        // Execute
        final ErrorResource result = new ErrorResource("Message", "Stack");

        // Verify
        assertEquals("Message", result.message);
        assertEquals("Stack", result.stack);
    }

    @Test
    void ErrorResource_of() {
        // Setup
        final Exception ex = new Exception("Message");

        // Execute
        final ErrorResource result = ErrorResource.of(ex);

        // Verify
        assertEquals("Message", result.message);
        assertTrue(result.stack.contains("Message"));
        assertTrue(result.stack.contains(Exception.class.getName()));
    }

    @Test
    void json() throws Exception {
        // Setup
        Map<String, Integer>  data     = HashMap.of("a", 2);
        ByteArrayOutputStream stream   = new ByteArrayOutputStream();
        Headers               headers  = mock(Headers.class);
        HttpExchange          exchange = mock(HttpExchange.class);
        doReturn(headers).when(exchange).getResponseHeaders();
        doReturn(stream).when(exchange).getResponseBody();

        // Execute
        HttpExchange result = Http.json(exchange, data);

        // Verify
        assertSame(result, exchange);
        verify(headers).set(Header.CONTENT_TYPE, ContentType.APPLICATION_JSON);
        verify(exchange).sendResponseHeaders(200, 13L);
        assertEquals("{\n  \"a\" : 2\n}", stream.toString());
    }

    @Test
    void extract() {
        // Setup
        ByteArrayInputStream stream   = new ByteArrayInputStream("{\n  \"a\" : 2\n}".getBytes());
        HttpExchange         exchange = mock(HttpExchange.class);

        doReturn(stream).when(exchange).getRequestBody();

        // Execute
        @SuppressWarnings("unchecked")
        java.util.Map<String, Integer> result = Http.extract(exchange, java.util.HashMap.class);

        // Verify
        assertEquals(
                HashMap.of("a", 2).toJavaMap(),
                result);
    }

    @Test
    void subContextPath() throws Exception {
        // Setup
        URI          uri      = new URI("http://example.com/step/test?a=23");
        String       ctxPath  = "/step";
        HttpContext  context  = mock(HttpContext.class);
        HttpExchange exchange = mock(HttpExchange.class);

        doReturn(uri).when(exchange).getRequestURI();
        doReturn(context).when(exchange).getHttpContext();
        doReturn(ctxPath).when(context).getPath();

        // Execute
        String result = Http.subContextPath(exchange);

        // Verify
        assertEquals("test", result);
    }

    @Test
    void subContextPath_nomatch() throws Exception {
        // Setup
        URI          uri      = new URI("test?a=23");
        String       ctxPath  = "step";
        HttpContext  context  = mock(HttpContext.class);
        HttpExchange exchange = mock(HttpExchange.class);

        doReturn(uri).when(exchange).getRequestURI();
        doReturn(context).when(exchange).getHttpContext();
        doReturn(ctxPath).when(context).getPath();

        // Execute
        String result = Http.subContextPath(exchange);

        // Verify
        assertEquals("test", result);
    }

    @Test
    void queryParams() throws Exception {
        // Setup
        URI          uri      = new URI("http://example.com/step/test?a=23&b=c&a=12%2A&=23&a");
        HttpExchange exchange = mock(HttpExchange.class);

        doReturn(uri).when(exchange).getRequestURI();

        // Execute
        Map<String, List<String>> result = Http.queryParams(exchange);

        // Verify
        @SuppressWarnings("unchecked")
        Map<String, List<String>> expect = LinkedHashMap.ofEntries(
                new Tuple2<>("a", List.of("23", "12*", "")),
                new Tuple2<>("b", List.of("c")));

        assertEquals(expect, result);

    }

    @Test
    void errorHandler() throws Exception {
        // Setup
        HttpExchange exchange = mock(HttpExchange.class);
        HttpHandler  handler  = mock(HttpHandler.class);
        doNothing().when(handler).handle(exchange);

        // Execute
        Http.errorHandler(handler).handle(exchange);

        // Verify
        verifyNoInteractions(exchange);
    }

    @Test
    void errorHandler_Error() throws Exception {
        // Setup
        Error                 error    = new Error(ErrorCode.NOT_FOUND, "Message234");
        ByteArrayOutputStream stream   = new ByteArrayOutputStream();
        Headers               headers  = mock(Headers.class);
        HttpExchange          exchange = mock(HttpExchange.class);
        HttpHandler           handler  = mock(HttpHandler.class);

        doReturn(headers).when(exchange).getResponseHeaders();
        doReturn(stream).when(exchange).getResponseBody();
        doThrow(error).when(handler).handle(exchange);

        // Execute
        Http.errorHandler(handler).handle(exchange);

        // Verify
        verify(headers).set(Header.CONTENT_TYPE, ContentType.APPLICATION_JSON);
        verify(exchange).sendResponseHeaders(eq(ErrorCode.NOT_FOUND), anyLong());
        assertTrue(stream.toString().contains("Message234"));
    }

    @Test
    void errorHandler_Exception() throws Exception {
        // Setup
        RuntimeException      error    = new RuntimeException("Message123");
        ByteArrayOutputStream stream   = new ByteArrayOutputStream();
        Headers               headers  = mock(Headers.class);
        HttpExchange          exchange = mock(HttpExchange.class);
        HttpHandler           handler  = mock(HttpHandler.class);

        doReturn(headers).when(exchange).getResponseHeaders();
        doReturn(stream).when(exchange).getResponseBody();
        doThrow(error).when(handler).handle(exchange);

        // Execute
        Http.errorHandler(handler).handle(exchange);

        // Verify
        verify(headers).set(Header.CONTENT_TYPE, ContentType.APPLICATION_JSON);
        verify(exchange).sendResponseHeaders(eq(ErrorCode.INTERNAL_SERVER_ERROR), anyLong());
        assertTrue(stream.toString().contains("Message123"));
    }
}
