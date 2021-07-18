package org.statemach.db.schema;

import java.util.Objects;

import org.statemach.util.Java;

public class DataType {
    public final String name;

    public DataType(String name) {
        this.name = name;
    }

    public static DataType unsupported(String name) {
        return new DataType(name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name);
    }

    @Override
    public String toString() {
        return "DataType@{name: '" + name + "'}";
    }
}
