package com.yg.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.statemach.util.Java;
import org.statemach.util.Json;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;

public interface Rest {

    static interface Header {
        final static String CONTENT_TYPE = "Content-Type";
    }

    static interface ContentType {
        final static String APPLICATION_JSON = "application/json";
    }

    static interface ErrorCode {
        final static int OK                 = 200;
        final static int BAD_REQUEST        = 400;
        final static int UNAUTHORIZED       = 401;
        final static int FORBIDDEN          = 403;
        final static int NOT_FOUND          = 404;
        final static int METHOD_NOT_ALLOWED = 405;
        final static int NOT_ACCEPTABLE     = 406;

        final static int INTERNAL_SERVER_ERROR = 500;
        final static int NOT_IMPLEMENTED       = 501;
        final static int SERVICE_UNAVAILABLE   = 503;
    }

    @SuppressWarnings("serial")
    static class Error extends RuntimeException {
        public final int errorCode;

        public Error(int errorCode, String format, Object... parameters) {
            super(Java.format(format, parameters));
            this.errorCode = errorCode;
        }

        public Error(int errorCode, Throwable ex) {
            super(ex);
            this.errorCode = errorCode;
        }

        public Error(int errorCode, Throwable ex, String format, Object... parameters) {
            super(Java.format(format, parameters), ex);
            this.errorCode = errorCode;
        }
    }

    static class ErrorResource {
        public final String message;
        public final String stack;

        ErrorResource(String message, String stack) {
            this.message = message;
            this.stack = stack;
        }

        public static ErrorResource of(Throwable ex) {
            return new ErrorResource(ex.getMessage(), Java.toString(ex));
        }
    }

    static HttpExchange json(HttpExchange exchange, Object data) {
        return json(exchange, 200, data);
    }

    static HttpExchange json(HttpExchange exchange, int returnCode, Object data) {
        return Java.soft(() -> {
            byte[] binary = Json.MAPPER.writeValueAsBytes(data);
            exchange.getResponseHeaders().set(Header.CONTENT_TYPE, ContentType.APPLICATION_JSON);
            exchange.sendResponseHeaders(returnCode, binary.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(binary);
            }
            return exchange;
        });
    }

    static <T> T extract(HttpExchange exchange, Class<T> type) {
        return Java.soft(() -> {
            try (InputStream input = exchange.getRequestBody()) {
                return Json.MAPPER.readValue(input, type);
            }
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
            .filter(p -> !p[0].isEmpty())
            .groupBy(a -> a[0])
            .mapValues(l -> l
                .map(a -> 2 == a.length
                        ? Java.soft(() -> URLDecoder.decode(a[1], StandardCharsets.UTF_8))
                        : "")
                .toList());
    }

    static HttpHandler errorHandler(HttpHandler handler) {
        return exchange -> {
            try {
                handler.handle(exchange);
            } catch (Error ex) {
                json(exchange, ex.errorCode, ErrorResource.of(ex));
            } catch (Throwable ex) {
                json(exchange, ErrorCode.INTERNAL_SERVER_ERROR, ErrorResource.of(ex));
            }
        };
    }

    static void respondeError(int returnCode, Throwable ex) {
        Java.toString(ex);
    }
}
