package org.statemach.db.graphql;

import java.util.Objects;

import org.statemach.util.Java;

import io.vavr.collection.List;

public class OrderBy {

    final List<String> path;
    final boolean      assending;

    public OrderBy(List<String> path, boolean assending) {
        this.path = path;
        this.assending = assending;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, assending);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.path, t -> t.assending);
    }

    @Override
    public String toString() {
        return "OrderBy@{path: " + path +
                ", assending: " + assending +
                "}";
    }
}
