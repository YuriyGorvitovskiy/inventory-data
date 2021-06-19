package org.statemach.db.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.vavr.Tuple2;

@FunctionalInterface
public interface Extract<T> {

    /// return next position
    Tuple2<T, Integer> get(ResultSet rs, int pos) throws SQLException;
}