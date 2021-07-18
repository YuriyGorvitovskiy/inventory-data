package org.statemach.db.graphql;

import org.statemach.db.sql.Select;

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

}
