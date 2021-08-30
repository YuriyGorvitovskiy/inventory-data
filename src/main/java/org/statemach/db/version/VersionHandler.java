package org.statemach.db.version;

import java.io.IOException;
import java.time.Instant;

import org.statemach.util.Http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class VersionHandler implements HttpHandler {

    public static class Version {
        public final String  name    = "statemach-data";
        public final String  version = "1.0.0";
        public final Instant time    = Instant.now();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        Http.json(exchange, new Version());
    }

}
