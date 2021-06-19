package org.statemach.db.schema;

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
}