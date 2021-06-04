package com.yg.inventory.model.db;

import com.yg.util.DB.DataType;

public class Column {
    public final String   name;
    public final DataType type;

    Column(String name, DataType type) {
        this.name = name;
        this.type = type;
    }
}
