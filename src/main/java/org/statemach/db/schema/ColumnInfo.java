package org.statemach.db.schema;

import java.util.Objects;

import org.statemach.util.Java;

import io.vavr.control.Option;

public class ColumnInfo {
    public final String          name;
    public final DataType        type;
    public final Option<Integer> size;

    public ColumnInfo(String name, DataType type, Option<Integer> size) {
        this.name = name;
        this.type = type;
        this.size = size;
    }

    public static ColumnInfo of(String name, DataType type) {
        return new ColumnInfo(name, type, Option.none());
    }

    public static ColumnInfo of(String name, DataType type, Integer size) {
        return new ColumnInfo(name, type, Option.of(size));
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, size);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name, t -> t.type, t -> t.size);
    }

    @Override
    public String toString() {
        return "ColumnInfo@{name: " + name + ", type: " + type + (null != size ? ", size: " + size : "") + "}";
    }
}
