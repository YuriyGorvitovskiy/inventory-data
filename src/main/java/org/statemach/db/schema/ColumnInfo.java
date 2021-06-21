package org.statemach.db.schema;

import java.util.Objects;

import org.statemach.util.Java;

public class ColumnInfo {
    public final String   name;
    public final DataType type;

    private final int hash;

    public ColumnInfo(String name, DataType type) {
        this.name = name;
        this.type = type;

        this.hash = Objects.hash(name, type);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name, t -> t.type);
    }
}
