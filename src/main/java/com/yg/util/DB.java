package com.yg.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Option;

public interface DB {

    static interface Config {
        static final String DB_ADDRESS  = "DB_ADDRESS";
        static final String DB_PORT     = "DB_PORT";
        static final String DB_NAME     = "DB_NAME";
        static final String DB_USERNAME = "DB_USERNAME";
        static final String DB_PASSWORD = "DB_PASSWORD";
    }

    static BasicDataSource pool = create();

    static BasicDataSource create() {
        return create(HashMap.ofAll(System.getenv()));
    }

    static BasicDataSource create(Map<String, String> config) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(org.postgresql.Driver.class.getName());
        dataSource.setUrl("jdbc:postgresql://" +
                config.getOrElse(Config.DB_ADDRESS, "localhost") + ":" +
                config.getOrElse(Config.DB_PORT, "31703") + "/" +
                config.getOrElse(Config.DB_NAME, "inventory"));
        dataSource.setUsername(config.getOrElse(Config.DB_USERNAME, "admin"));
        dataSource.setPassword(config.getOrElse(Config.DB_PASSWORD, "M9bmiR8iuod9wFHskgFu"));

        return dataSource;
    }

    public static <T, E extends Exception> T call(FunctionEx<Connection, T, E> f) {
        return Java.soft(() -> {
            try (Connection connection = pool.getConnection()) {
                return f.apply(connection);
            }
        });
    }

    public static <E extends Exception> void run(ConsumerEx<Connection, E> f) {
        Java.soft(() -> {
            try (Connection connection = pool.getConnection()) {
                f.accept(connection);
            }
        });
    }

    public static <E extends Exception> void execute(String statement, ConsumerEx<PreparedStatement, E> p) {
        run(c -> {
            try (PreparedStatement ps = c.prepareStatement(statement)) {
                p.accept(ps);
                ps.execute();
            }
        });
    }

    public static <R, E extends SQLException> List<R> executeQuery(String statement,
                                                                   ConsumerEx<PreparedStatement, E> prep,
                                                                   FunctionEx<ResultSet, R, E> row) {
        return call(c -> executeQuery(c, statement, prep, row));
    }

    static <R, E extends Exception> List<R> executeQuery(Connection connection,
                                                         String statement,
                                                         ConsumerEx<PreparedStatement, E> prep,
                                                         FunctionEx<ResultSet, R, E> row) throws E, SQLException {
        try (PreparedStatement ps = connection.prepareStatement(statement)) {
            prep.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return Stream
                    .iterate(() -> Java.soft(() -> rs.next() ? Option.of(rs) : Option.none()))
                    .map(s -> Java.soft(() -> row.apply(s)))
                    .toList();
            }
        }
    }
}
