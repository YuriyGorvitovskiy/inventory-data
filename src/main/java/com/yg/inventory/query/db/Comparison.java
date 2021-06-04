package com.yg.inventory.query.db;

import io.vavr.collection.List;

public class Comparison {
    public static enum Op {
        IS_NULL,
        EQUAL,
        IN,
        LESSER,
        GREATER,
        BETWEEN,
        LIKE,
        TEXT_SEARCH,
    }

    public final List<String> path;
    public final boolean      inverse;
    public final Op           op;
    public final List<Object> value;

    public Comparison(List<String> path,
                      boolean inverse,
                      Op op,
                      List<Object> value) {
        this.path = path;
        this.inverse = inverse;
        this.op = op;
        this.value = value;
    }

}
