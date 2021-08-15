package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.statemach.util.Http;

import com.sun.net.httpserver.HttpExchange;

import graphql.GraphQL;
import io.vavr.collection.LinkedHashMap;

public class GraphQLHandler_UnitTest {

    final GraphQL        graphQL  = mock(GraphQL.class);
    final HttpExchange   exchange = mock(HttpExchange.class);
    final GraphQLHandler subject  = spy(new GraphQLHandler(graphQL));

    @Test
    void handle_get() throws Exception {
        // Setup
        final URI uri = new URI("http://example.com/graphql?query=%7B%7D&variables=%7B%22a%22%3A%22b%22%7D&operationName=test");
        doReturn("get").when(exchange).getRequestMethod();
        doReturn(uri).when(exchange).getRequestURI();
        doNothing().when(subject).execute(any(), any());

        // Execute
        subject.handle(exchange);

        // Verify
        ArgumentCaptor<GraphQLHandler.Input> input = ArgumentCaptor.forClass(GraphQLHandler.Input.class);
        verify(subject).execute(same(exchange), input.capture());
        assertEquals("{}", input.getValue().query);
        assertEquals(LinkedHashMap.of("a", "b"), input.getValue().variables);
        assertEquals("test", input.getValue().operationName);
    }

    @Test
    void handle_get_noQuery() throws Exception {
        // Setup
        final URI uri = new URI("http://example.com/graphql?variables=%7B%22a%22%3A%22b%22%7D&operationName=test");
        doReturn("get").when(exchange).getRequestMethod();
        doReturn(uri).when(exchange).getRequestURI();
        doNothing().when(subject).execute(any(), any());

        // Execute & Verify
        assertThrows(Http.Error.class, () -> subject.handle(exchange));
    }

    @Test
    void handle_post() throws Exception {
        // Setup
        String body = "{\"query\":\"{}\",\"operationName\":\"test\",\"variables\":{\"a\":\"b\"}}";
        try (InputStream data = new ByteArrayInputStream(body.getBytes());) {
            doReturn("post").when(exchange).getRequestMethod();
            doReturn(data).when(exchange).getRequestBody();
            doNothing().when(subject).execute(any(), any());

            // Execute
            subject.handle(exchange);

            // Verify
            ArgumentCaptor<GraphQLHandler.Input> input = ArgumentCaptor.forClass(GraphQLHandler.Input.class);
            verify(subject).execute(same(exchange), input.capture());
            assertEquals("{}", input.getValue().query);
            assertEquals(LinkedHashMap.of("a", "b"), input.getValue().variables);
            assertEquals("test", input.getValue().operationName);
        }
    }

}
