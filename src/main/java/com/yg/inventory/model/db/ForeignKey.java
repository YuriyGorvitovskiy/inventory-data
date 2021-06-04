package com.yg.inventory.model.db;

public class ForeignKey {

    public final String name;
    public final String fromTable;
    public final String fromColumn;
    public final String toTable;
    public final String toColumn;

    public ForeignKey(String name, String fromTable, String fromColumn, String toTable, String toColumn) {
        this.name = name;
        this.fromTable = fromTable;
        this.fromColumn = fromColumn;
        this.toTable = toTable;
        this.toColumn = toColumn;
    }
}