package org.statemach.db.schema;

import java.util.Objects;

import org.statemach.util.Java;

import io.vavr.collection.List;

public class PrimaryKey {

    public final String       name;
    public final String       table;
    public final List<String> columns;

    public PrimaryKey(String name, String table, List<String> columns) {
        this.name = name;
        this.table = table;
        this.columns = columns;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, table, columns);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name, t -> t.table, t -> t.columns);
    }

    @Override
    public String toString() {
        return "PrimaryKey@{name: " + name + ", table: " + table + "}";
    }

}