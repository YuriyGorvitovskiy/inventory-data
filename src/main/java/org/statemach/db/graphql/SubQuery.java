package org.statemach.db.graphql;

import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.TableInfo;

import graphql.schema.SelectedField;
import io.vavr.collection.List;

public class SubQuery {
    final String             name;
    final List<ExtractValue> extracts;
    final List<String>       path;
    final ForeignKey         incoming;
    final TableInfo          table;
    final GraphQLField       field;

    SubQuery(String name,
             List<String> path,
             List<ExtractValue> extracts,
             ForeignKey incoming,
             TableInfo table,
             GraphQLField field) {
        this.name = name;
        this.path = path;
        this.extracts = extracts;
        this.incoming = incoming;
        this.table = table;
        this.field = field;
    }

    public static SubQuery of(List<String> path,
                              List<ExtractValue> extracts,
                              ForeignKey incoming,
                              TableInfo table,
                              SelectedField field) {
        return new SubQuery(path
            .mkString(ExtractValue.NAME_DELIMITER), path, extracts, incoming, table, GraphQLField.of(field));
    }
}
