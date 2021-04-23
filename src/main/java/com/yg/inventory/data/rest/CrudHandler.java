package com.yg.inventory.data.rest;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.yg.util.DB;
import com.yg.util.Rest;

import io.vavr.collection.List;

public class CrudHandler implements HttpHandler {

    public static class Paint {
        public final long   id;
        public final String name;
        public final String sku;
        public final int    palette_number;
        public final String color_hex;

        public Paint(long id,
                     String name,
                     String sku,
                     int palette_number,
                     String color_hex) {
            this.id = id;
            this.name = name;
            this.sku = sku;
            this.palette_number = palette_number;
            this.color_hex = color_hex;
        }
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        List<Paint> result = DB.executeQuery(
                "SELECT id, name, sku, palette_number, color_hex FROM paint FETCH FIRST 100 ROWS ONLY",
                (ps) -> {},
                (rs) -> new Paint(
                        rs.getLong(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getInt(4),
                        rs.getString(5)));

        Rest.json(exchange, result);
    }

}
