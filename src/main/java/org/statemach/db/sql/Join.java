package org.statemach.db.sql;

public class Join {

    public static enum Kind {
        CROSS(SQL.CROSS_JOIN),
        INNER(SQL.INNER_JOIN),
        LEFT(SQL.LEFT_JOIN),
        RIGHT(SQL.RIGHT_JOIN),
        FULL(SQL.FULL_JOIN);

        public final String sql;

        Kind(String sql) {
            this.sql = sql;
        }
    }

    public final Kind      kind;
    public final Condition condition;

    public Join(Kind kind, Condition condition) {
        this.kind = kind;
        this.condition = condition;
    }
}
