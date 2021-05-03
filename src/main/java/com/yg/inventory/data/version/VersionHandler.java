package com.yg.inventory.data.version;

import java.io.IOException;
import java.time.Instant;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.yg.util.Rest;

public class VersionHandler implements HttpHandler {

    public static class Version {
        public final String  name    = "Inventory Data";
        public final String  version = "0.0.6";
        public final Instant time    = Instant.now();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        Rest.json(exchange, new Version());
    }

}
