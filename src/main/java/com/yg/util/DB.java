package com.yg.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.dbcp2.BasicDataSource;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
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

    public enum DataType {
        BIGINT,
        BOOLEAN,
        INTEGER,
        TEXT_SEARCH_VECTOR,
        VARCHAR,
        UUID;

        public static final Map<String, DataType> FROM_DB_NAME = HashMap.ofEntries(
                new Tuple2<>("bigint", BIGINT),
                new Tuple2<>("boolean", BOOLEAN),
                new Tuple2<>("character varying", VARCHAR),
                new Tuple2<>("integer", INTEGER),
                new Tuple2<>("tsvector", TEXT_SEARCH_VECTOR),
                new Tuple2<>("uuid", UUID));

        public static final Map<DataType, String> TO_DB_NAME = FROM_DB_NAME.toMap(t -> new Tuple2<>(t._2, t._1));

        public static final Set<DataType> SUPPORTED = HashSet.of(BIGINT, BOOLEAN, INTEGER, VARCHAR, UUID);

    }

    @FunctionalInterface
    static interface Inject {
        /// return next position
        int set(PreparedStatement ps, int pos) throws SQLException;
    }

    static interface Injects {
        static final Inject                               NOTHING = (ps, i) -> i;
        static final Function<Boolean, Inject>            BOOLEAN = (v) -> (ps, i) -> {
                                                                      if (null == v) {
                                                                          ps.setNull(i, Types.BOOLEAN);
                                                                      } else {
                                                                          ps.setBoolean(i, v);
                                                                      }
                                                                      return i + 1;
                                                                  };
        static final Function<Integer, Inject>            INTEGER = (v) -> (ps, i) -> {
                                                                      if (null == v) {
                                                                          ps.setNull(i, Types.INTEGER);
                                                                      } else {
                                                                          ps.setInt(i, v);
                                                                      }
                                                                      return i + 1;
                                                                  };
        static final Function<Long, Inject>               LONG    = (v) -> (ps, i) -> {
                                                                      if (null == v) {
                                                                          ps.setNull(i, Types.BIGINT);
                                                                      } else {
                                                                          ps.setLong(i, v);
                                                                      }
                                                                      return i + 1;
                                                                  };
        static final Function<String, Inject>             STRING  = (v) -> (ps, i) -> {
                                                                      if (null == v) {
                                                                          ps.setNull(i, Types.VARCHAR);
                                                                      } else {
                                                                          ps.setString(i, v);
                                                                      }
                                                                      return i + 1;
                                                                  };
        static final Function<java.util.UUID, Inject>     UUID    = (v) -> (ps, i) -> {
                                                                      if (null == v) {
                                                                          ps.setObject(i, new java.util.UUID(0L, 0L));
                                                                      } else {
                                                                          ps.setObject(i, v);
                                                                      }
                                                                      return i + 1;
                                                                  };
        static final BiFunction<DataType, Seq<?>, Inject> ARRAY   = (t, v) -> (ps, i) -> {
                                                                      ps.setArray(i,
                                                                              ps.getConnection().createArrayOf(
                                                                                      DataType.TO_DB_NAME.get(t).get(),
                                                                                      v.toJavaArray()));
                                                                      return i + 1;
                                                                  };

        static interface Str {
            static final Function<String, Inject> INTEGER = (v) -> Injects.INTEGER.apply(Integer.parseInt(v));
            static final Function<String, Inject> BOOLEAN = (v) -> Injects.BOOLEAN.apply(Boolean.parseBoolean(v));
            static final Function<String, Inject> LONG    = (v) -> Injects.LONG.apply(Long.parseLong(v));
            static final Function<String, Inject> STRING  = Injects.STRING;
            static final Function<String, Inject> UUID    = (v) -> Injects.UUID
                .apply(Java.isEmpty(v) ? null : java.util.UUID.fromString(v));
        }

        static interface Obj {
            static final Function<Object, Inject> BOOLEAN = (v) -> Injects.BOOLEAN
                .apply(v instanceof Boolean ? ((Boolean) v).booleanValue() : null);
            static final Function<Object, Inject> INTEGER = (v) -> Injects.INTEGER
                .apply(v instanceof Number ? ((Number) v).intValue() : null);
            static final Function<Object, Inject> LONG    = (v) -> Injects.LONG
                .apply(v instanceof Number ? ((Number) v).longValue() : null);
            static final Function<Object, Inject> STRING  = (v) -> Injects.STRING
                .apply(null == v ? null : v.toString());
            static final Function<Object, Inject> UUID    = (v) -> Injects.UUID
                .apply(null == v
                        ? null
                        : v instanceof java.util.UUID
                                ? (java.util.UUID) v
                                : java.util.UUID.fromString(v.toString()));
        }
    }

    static final Map<DataType, Function<String, Inject>> DATA_TYPE_STRING_INJECT = HashMap.ofEntries(
            new Tuple2<>(DataType.BIGINT, Injects.Str.LONG),
            new Tuple2<>(DataType.BOOLEAN, Injects.Str.BOOLEAN),
            new Tuple2<>(DataType.INTEGER, Injects.Str.INTEGER),
            new Tuple2<>(DataType.VARCHAR, Injects.Str.STRING),
            new Tuple2<>(DataType.UUID, Injects.Str.UUID));

    static final Map<DataType, Function<Object, Inject>> DATA_TYPE_INJECT = HashMap.ofEntries(
            new Tuple2<>(DataType.BIGINT, Injects.Obj.LONG),
            new Tuple2<>(DataType.BOOLEAN, Injects.Obj.BOOLEAN),
            new Tuple2<>(DataType.INTEGER, Injects.Obj.INTEGER),
            new Tuple2<>(DataType.VARCHAR, Injects.Obj.STRING),
            new Tuple2<>(DataType.UUID, Injects.Obj.UUID));

    @FunctionalInterface
    static interface Extract<T> {
        T get(ResultSet rs, int pos) throws SQLException;
    }

    static interface Extracts {
        static final Extract<Boolean>        BOOLEAN = (rs, i) -> {
                                                         boolean value = rs.getBoolean(i);
                                                         return rs.wasNull() ? null : value;
                                                     };
        static final Extract<Integer>        INTEGER = (rs, i) -> {
                                                         int value = rs.getInt(i);
                                                         return rs.wasNull() ? null : value;
                                                     };
        static final Extract<Long>           LONG    = (rs, i) -> {
                                                         long value = rs.getLong(i);
                                                         return rs.wasNull() ? null : value;
                                                     };
        static final Extract<String>         STRING  = (rs, i) -> {
                                                         String value = rs.getString(i);
                                                         return rs.wasNull() ? null : value;
                                                     };
        static final Extract<java.util.UUID> UUID    = (rs, i) -> {
                                                         java.util.UUID value = (java.util.UUID) rs.getObject(i);
                                                         return rs.wasNull() ? null : value;
                                                     };
    }

    static final Map<DataType, Extract<?>> DATA_TYPE_EXTRACT = HashMap.ofEntries(
            new Tuple2<>(DataType.BIGINT, Extracts.LONG),
            new Tuple2<>(DataType.BOOLEAN, Extracts.BOOLEAN),
            new Tuple2<>(DataType.INTEGER, Extracts.INTEGER),
            new Tuple2<>(DataType.VARCHAR, Extracts.STRING),
            new Tuple2<>(DataType.UUID, Extracts.UUID));

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

    static <T, E extends Exception> T call(FunctionEx<Connection, T, E> f) {
        return Java.soft(() -> {
            try (Connection connection = pool.getConnection()) {
                return f.apply(connection);
            }
        });
    }

    static <E extends Exception> void run(ConsumerEx<Connection, E> f) {
        Java.soft(() -> {
            try (Connection connection = pool.getConnection()) {
                f.accept(connection);
            }
        });
    }

    static <E extends Exception> void execute(String statement, ConsumerEx<PreparedStatement, E> p) {
        run(c -> {
            try (PreparedStatement ps = c.prepareStatement(statement)) {
                p.accept(ps);
                ps.execute();
            }
        });
    }

    static <R, E extends SQLException> List<R> query(String statement,
                                                     ConsumerEx<PreparedStatement, E> prep,
                                                     FunctionEx<ResultSet, R, E> row) {
        return call(c -> query(c, statement, prep, row));
    }

    static <R, E extends Exception> List<R> query(Connection connection,
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

    static Inject fold(Seq<Inject> injects) {
        return injects.foldLeft(Injects.NOTHING, (f, j) -> (ps, i) -> j.set(ps, f.set(ps, i)));
    }

    static Tuple2<String, DB.Inject> andSqlInjects(Seq<Tuple2<String, DB.Inject>> conditions) {
        if (conditions.isEmpty()) {
            return new Tuple2<>("1 = 1", DB.Injects.NOTHING);
        }
        if (1 == conditions.size()) {
            return conditions.get();
        }
        return new Tuple2<>("(" + conditions.map(t -> t._1).mkString(") AND (") + ")", DB.fold(conditions.map(t -> t._2)));
    }

    static <T> Tuple2<String, DB.Inject> conditionSqlInject(String column,
                                                            DataType dataType,
                                                            Map<DataType, Function<T, Inject>> injectors,
                                                            List<T> values) {
        if (DB.DataType.TEXT_SEARCH_VECTOR.equals(dataType)) {
            return new Tuple2<>("websearch_to_tsquery('english', ?) @@ " + column,
                    DB.Injects.Str.STRING.apply(values.mkString(" ")));
        }
        if (values.isEmpty()) {
            return new Tuple2<>(column + " IS NULL", DB.Injects.NOTHING);
        }

        Function<T, Inject> injector = injectors.get(dataType).get();
        if (1 == values.size()) {
            return new Tuple2<>(column + " = ?", injector.apply(values.get()));
        }
        return new Tuple2<>(column + " IN (" + Java.repeat("?", ", ", values.size()) + ")", DB.fold(values.map(injector)));
    }

}
