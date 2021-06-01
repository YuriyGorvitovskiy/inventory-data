package com.yg.inventory.data.db;

public class Column<T> {
    public final String alias;
    public final String column;
    public final String name;
    public final T      _1;

    public Column(String alias, String column, String name, T _1) {
        this.alias = alias;
        this.column = column;
        this.name = name;
        this._1 = _1;
    }
}
