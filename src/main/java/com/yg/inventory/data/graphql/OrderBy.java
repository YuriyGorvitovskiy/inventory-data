package com.yg.inventory.data.graphql;

import com.yg.inventory.data.db.Column;

import io.vavr.collection.List;

public class OrderBy {

    static final int PARAM_LIMIT = 7;

    final List<String> path;
    final boolean      assending;

    public OrderBy(List<String> path, boolean assending) {
        this.path = path;
        this.assending = assending;
    }

    public Column<Boolean> buildCondition(String tableAlias) {
        String columnName = path.last();
        return new Column<>(tableAlias, columnName, columnName, assending);
    }

}
