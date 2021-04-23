package com.yg.inventory.data.server;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

import com.sun.net.httpserver.HttpServer;

import com.yg.inventory.data.rest.CrudHandler;
import com.yg.inventory.data.rest.VersionHandler;
import com.yg.util.Java;

public class Main {
    static final int HTTP_PORT = 3702;

    public static Supplier<Main> factory = () -> new Main();

    HttpServer build() throws Exception {
        HttpServer server = HttpServer.create();
        server.createContext("/", new VersionHandler());
        server.createContext("/crud/", new CrudHandler());
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
