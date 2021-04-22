package com.yg.util;

import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;

public interface Rest {
    public static interface Header {
        public final static String CONTENT_TYPE = "Content-Type";
    }

    public static interface ContentType {
        public final static String APPLICATION_JSON = "application/json";
    }

    public static HttpExchange json(HttpExchange exchange, Object data) {
        return Java.soft(() -> {
            byte[] binary = Json.MAPPER.writeValueAsBytes(data);
            exchange.getResponseHeaders().set(Header.CONTENT_TYPE, ContentType.APPLICATION_JSON);
            exchange.sendResponseHeaders(200, binary.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(binary);
            }
            return exchange;
        });
    }
}
