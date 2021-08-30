package org.statemach.db.version;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.statemach.util.Mutable;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

public class VersionHandler_UnitTest {

    final VersionHandler subject = new VersionHandler();

    final HttpExchange exchange        = mock(HttpExchange.class);
    final Headers      responseHeaders = mock(Headers.class);

    final ByteArrayOutputStream         output       = new ByteArrayOutputStream();
    final java.util.Map<String, String> headers      = new java.util.HashMap<>();
    final Mutable<Integer>              resultCode   = new Mutable<>(null);
    final Mutable<Long>                 resultLength = new Mutable<>(null);

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
    void handle() throws Exception {
        // Execute
        subject.handle(exchange);

        // Verify
        assertEquals(200, resultCode.get());
        assertTrue(resultLength.get() > 10);
    }
}
