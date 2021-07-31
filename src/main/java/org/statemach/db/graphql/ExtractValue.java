package org.statemach.db.graphql;

import java.util.Objects;

import org.statemach.db.schema.DataType;
import org.statemach.util.Java;

import io.vavr.collection.List;

public class ExtractValue {
    static final String NAME_DELIMITER = ".";

    final String       name;
    final List<String> path;
    final DataType     type;

    ExtractValue(String name, List<String> path, DataType type) {
        this.name = name;
        this.path = path;
        this.type = type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path, type);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name, t -> t.path, t -> t.type);
    }

    @Override
    public String toString() {
        return "ExtractValue@{name: " + name +
                ", path: " + path +
                ", type: " + type +
                "}";
    }

    public static ExtractValue of(List<String> path, DataType type) {
        return new ExtractValue(path.mkString(NAME_DELIMITER), path, type);
    }

    public static String nameOf(List<String> path) {
        return path.mkString(NAME_DELIMITER);
    }

}
