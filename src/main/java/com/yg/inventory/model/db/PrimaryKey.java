package com.yg.inventory.model.db;

public class PrimaryKey {

    public final String name;
    public final String table;
    public final String column;

    public PrimaryKey(String name, String table, String column) {
        this.name = name;
        this.table = table;
        this.column = column;
    }
}