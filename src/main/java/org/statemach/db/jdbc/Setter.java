package org.statemach.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

@FunctionalInterface
public interface Setter<K> {

    void set(PreparedStatement ps, int i, K value) throws SQLException;

    static final Setter<Boolean>   BOOLEAN   = (ps, i, v) -> ps.setBoolean(i, v);
    static final Setter<Double>    DOUBLE    = (ps, i, v) -> ps.setDouble(i, v);
    static final Setter<Integer>   INTEGER   = (ps, i, v) -> ps.setInt(i, v);
    static final Setter<Long>      LONG      = (ps, i, v) -> ps.setLong(i, v);
    static final Setter<String>    STRING    = (ps, i, v) -> ps.setString(i, v);
    static final Setter<Timestamp> TIMESTAMP = (ps, i, v) -> ps.setTimestamp(i, v);
    static final Setter<Object>    OBJECT    = (ps, i, v) -> ps.setObject(i, v);
};
