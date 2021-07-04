package org.statemach.db.sql;

public class From {

    public final String table;
    public final String alias;

    public From(String table, String alias) {
        this.table = table;
        this.alias = alias;
    }
}
