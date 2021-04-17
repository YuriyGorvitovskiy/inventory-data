package com.yg.inventory.data.service;

import java.util.function.Supplier;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

public class Service {

    public static Supplier<Service> factory = () -> new Service();

    protected Service() {
    }

    protected Undertow build() {
        return Undertow.builder()
            .addHttpListener(3702, "0.0.0.0")
            .setHandler(new HttpHandler() {
                @Override
                public void handleRequest(final HttpServerExchange exchange) throws Exception {
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{'name': 'Inventory Data'}");
                }
            }).build();
    }

    public void run() {
        build().start();
    }

    public static void main(final String[] args) {
        factory.get().run();
    }
}
