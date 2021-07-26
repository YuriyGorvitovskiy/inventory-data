package org.statemach.db.schema;

import java.util.Objects;

import org.statemach.util.Java;

import io.vavr.collection.List;

public class CompositeType {
    public final String           name;
    public final List<ColumnInfo> fields;

    public CompositeType(String name, List<ColumnInfo> fields) {
        this.name = name;
        this.fields = fields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fields);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name, t -> t.fields);
    }

    @Override
    public String toString() {
        return "CompositeType@{name: " + name + ", fields: " + fields + "}";
    }

}
