package org.statemach.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface Inject {

    /// return next position
    int set(PreparedStatement ps, int pos) throws SQLException;

}
