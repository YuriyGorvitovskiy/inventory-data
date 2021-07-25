package org.statemach.db.graphql;

import java.util.Objects;

import org.statemach.util.Java;

import io.vavr.collection.List;

public class ForeignKeyPath {
    public final List<String>       path;
    public final List<ExtractValue> extracts;

    public ForeignKeyPath(List<String> path, List<ExtractValue> extracts) {
        this.path = path;
        this.extracts = extracts;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, extracts);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.path, t -> t.extracts);
    }

    @Override
    public String toString() {
        return "ForeignKeyPath@{path: " + path +
                ", extracts: " + extracts +
                "}";
    }

}
