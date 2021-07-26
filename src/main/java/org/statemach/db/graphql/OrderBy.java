package org.statemach.db.graphql;

import java.util.Objects;

import org.statemach.db.sql.Select;
import org.statemach.util.Java;

import io.vavr.collection.List;

public class OrderBy {

    static final int PARAM_LIMIT = 7;

    final List<String> path;
    final boolean      assending;

    public OrderBy(List<String> path, boolean assending) {
        this.path = path;
        this.assending = assending;
    }

    public Select<Boolean> buildCondition(String tableAlias) {
        String columnName = path.last();
        return Select.of(tableAlias, columnName, assending);
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
