package org.statemach.db.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.statemach.util.ConsumerEx;
import org.statemach.util.FunctionEx;
import org.statemach.util.Java;

import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;

public class JDBC {

    final BasicDataSource pool;

    public JDBC(BasicDataSource pool) {
        this.pool = pool;
    }

    public <T, E extends Exception> T call(FunctionEx<Connection, T, E> processor) {
        return Java.soft(() -> {
            try (Connection connection = pool.getConnection()) {
                return processor.apply(connection);
            }
        });
    }

    public <E extends Exception> void run(ConsumerEx<Connection, E> processor) {
        Java.soft(() -> {
            try (Connection connection = pool.getConnection()) {
                processor.accept(connection);
            }
        });
    }

    public <E extends Exception> void execute(Connection connection,
                                              String statement,
                                              ConsumerEx<PreparedStatement, E> stuffing) {
        run(c -> {
            try (PreparedStatement ps = connection.prepareStatement(statement)) {
                stuffing.accept(ps);
                ps.execute();
            }
        });
    }

    public <E extends Exception> void execute(String statement, ConsumerEx<PreparedStatement, E> stuffing) {
        run(c -> {
            try (PreparedStatement ps = c.prepareStatement(statement)) {
                stuffing.accept(ps);
                ps.execute();
            }
        });
    }

    public <E extends Exception> void execute(String statement, Inject inject) {
        execute(statement, ps -> inject.set(ps, 1));
    }

    public <R, E extends Exception> List<R> query(Connection connection,
                                                  String statement,
                                                  ConsumerEx<PreparedStatement, E> stuffing,
                                                  FunctionEx<ResultSet, R, E> rowExtractor) throws E, SQLException {
        try (PreparedStatement ps = connection.prepareStatement(statement)) {
            stuffing.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return Stream
                    .iterate(() -> Java.soft(() -> rs.next() ? Option.of(rs) : Option.none()))
                    .map(s -> Java.soft(() -> rowExtractor.apply(s)))
                    .toList();
            }
        }
    }

    public <R, E extends Exception> List<R> query(String statement,
                                                  ConsumerEx<PreparedStatement, E> stuffing,
                                                  FunctionEx<ResultSet, R, E> extractor) {
        return call(c -> query(c, statement, stuffing, extractor));
    }

    public <R, E extends Exception> List<R> query(String statement, Inject inject, Extract<R> extract) {
        return call(c -> query(c, statement, ps -> inject.set(ps, 1), rs -> extract.get(rs, 1)._1));
    }

}
