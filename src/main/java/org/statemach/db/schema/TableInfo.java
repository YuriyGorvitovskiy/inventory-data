package org.statemach.db.schema;

import io.vavr.collection.Map;

public class TableInfo {

    public final String                  name;
    public final Map<String, ColumnInfo> columns;
    public final PrimaryKey              primary;
    public final Map<String, ForeignKey> incoming;
    public final Map<String, ForeignKey> outgoing;

    public TableInfo(String name,
                     Map<String, ColumnInfo> columns,
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
