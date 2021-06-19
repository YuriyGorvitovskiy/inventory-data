package org.statemach.db.schema;

import com.yg.util.DB.DataType;

public class ColumnInfo {
    public final String   name;
    public final DataType type;

    public ColumnInfo(String name, DataType type) {
        this.name = name;
        this.type = type;
    }
}
