package com.yg.inventory.data.server;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.function.Supplier;

import com.sun.net.httpserver.HttpServer;

import com.yg.util.Java;
import com.yg.util.Rest;

import io.vavr.collection.TreeMap;

public class Main {
    static final int HTTP_PORT = 3702;

    public static Supplier<Main> factory = () -> new Main();

    HttpServer build() throws Exception {
        HttpServer server = HttpServer.create();
        server.createContext("/",
                e -> Rest.json(e, TreeMap.of("name", "Inventory Data", "version", "0.0.2", "time", Instant.now())));
        server.setExecutor(null); // creates a default executor
        return server;
    }

    public void run() {
        Java.soft(() -> {
            HttpServer server = build();
            server.bind(new InetSocketAddress(HTTP_PORT), 0);
            server.start();
        });
    }

    public static void main(final String[] args) {
        factory.get().run();
    }
}
