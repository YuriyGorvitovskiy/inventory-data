package org.statemach.db.sql;

public class From {

    public final TableLike table;
    public final String    alias;

    public From(TableLike table, String alias) {
        this.table = table;
        this.alias = alias;
    }
}
