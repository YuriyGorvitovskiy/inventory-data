package org.statemach.db.schema;

import java.util.Objects;

import org.statemach.util.Java;

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

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.columns, this.primary, this.incoming, this.outgoing);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name, t -> t.columns, t -> t.primary, t -> t.incoming, t -> t.outgoing);
    }

    @Override
    public String toString() {
        return "TableInfo@{name: " + name + "}";
    }

}
