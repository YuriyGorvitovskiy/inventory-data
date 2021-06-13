package com.yg.inventory.data.graphql;

import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Table;

import graphql.schema.SelectedField;
import io.vavr.collection.List;

public class SubQuery {
    final String       name;
    final Extract      extract;
    final List<String> path;
    final ForeignKey   incoming;
    final Table        table;
    final GraphQLField field;

    SubQuery(String name, List<String> path, Extract extract, ForeignKey incoming, Table table, GraphQLField field) {
        this.name = name;
        this.path = path;
        this.extract = extract;
        this.incoming = incoming;
        this.table = table;
        this.field = field;
    }

    public static SubQuery of(List<String> path, Extract extract, ForeignKey incoming, Table table, SelectedField field) {
        return new SubQuery(path.mkString(Extract.NAME_DELIMITER), path, extract, incoming, table, GraphQLField.of(field));
    }
}
