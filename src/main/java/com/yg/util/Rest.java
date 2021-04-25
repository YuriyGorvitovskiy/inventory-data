package com.yg.util;

import java.io.OutputStream;
import java.net.URI;

import com.sun.net.httpserver.HttpExchange;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;

public interface Rest {
    static interface Header {
        public final static String CONTENT_TYPE = "Content-Type";
    }

    static interface ContentType {
        public final static String APPLICATION_JSON = "application/json";
    }

    static HttpExchange json(HttpExchange exchange, Object data) {
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

    static String subContextPath(HttpExchange exchange) {
        URI    uri      = exchange.getRequestURI();
        String ctxPath  = exchange.getHttpContext().getPath();
        String fullPath = uri.getPath();
        String subPath  = fullPath.startsWith(ctxPath) ? fullPath.substring(ctxPath.length()) : fullPath;
        return subPath.startsWith("/") ? subPath.substring(1) : subPath;
    }

    static Map<String, List<String>> queryParams(HttpExchange exchange) {
        URI uri = exchange.getRequestURI();
        return Stream.of(Java.toString(uri.getQuery()).split("&"))
            .map(p -> p.split("=", 2))
            .groupBy(a -> a[0])
            .mapValues(l -> l.map(a -> 2 == a.length ? a[1] : "").toList());
    }
}
