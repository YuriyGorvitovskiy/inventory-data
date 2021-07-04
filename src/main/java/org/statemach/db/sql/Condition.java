package org.statemach.db.sql;

import org.statemach.db.jdbc.Inject;

public class Condition {

    public static final Condition NONE = new Condition(SQL.TRIVIAL, Inject.NOTHING);

    public final String sql;
    public final Inject inject;

    public Condition(String sql, Inject inject) {
        this.sql = sql;
        this.inject = inject;
    }

}
