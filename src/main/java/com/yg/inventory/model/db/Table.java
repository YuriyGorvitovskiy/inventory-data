package com.yg.inventory.model.db;

import io.vavr.collection.Map;

public class Table {

    public final String                  name;
    public final Map<String, Column>     columns;
    public final PrimaryKey              primary;
    public final Map<String, ForeignKey> incoming;
    public final Map<String, ForeignKey> outgoing;

    Table(String name,
          Map<String, Column> columns,
          PrimaryKey primary,
          Map<String, ForeignKey> incoming,
          Map<String, ForeignKey> outgoing) {
        this.name = name;
        this.columns = columns;
        this.primary = primary;
        this.incoming = incoming;
        this.outgoing = outgoing;
    }
}
