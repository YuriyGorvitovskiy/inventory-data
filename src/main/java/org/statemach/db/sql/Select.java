package org.statemach.db.sql;

public class Select<T> {
    public final String from;
    public final String column;
    public final T      _1;

    public Select(String from, String column, T _1) {
        this.from = from;
        this.column = column;
        this._1 = _1;
    }

    public String sql() {
        return from + SQL.DOT + column;
    }
}
