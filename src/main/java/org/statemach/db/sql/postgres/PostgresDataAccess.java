package org.statemach.db.sql.postgres;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.jdbc.JDBC;
import org.statemach.db.sql.DataAccess;
import org.statemach.db.sql.SQL;
import org.statemach.util.Java;

import io.vavr.collection.Map;
import io.vavr.control.Option;

public class PostgresDataAccess implements DataAccess {

    static final String INSERT = Java.resource("Insert.sql");
    static final String MERGE  = Java.resource("Merge.sql");
    static final String UPDATE = Java.resource("Update.sql");
    static final String DELETE = Java.resource("Delete.sql");
    static final String SELECT = Java.resource("Select.sql");

    public final JDBC   jdbc;
    public final String schema;

    public PostgresDataAccess(JDBC jdbc, String schema) {
        this.jdbc = jdbc;
        this.schema = schema;
    }

    @Override
    public void insert(String table, Map<String, Inject> values) {
        String sql = Java.format(INSERT,
                schema,
                table,
                values.map(t -> t._1).mkString(SQL.COMMA),
                Java.repeat(SQL.PARAM, SQL.COMMA, values.size()),
                "");

        jdbc.execute(sql, ps -> Inject.inject(ps, 1, values));
    }

    @Override
    public Map<String, Object> insert(String table,
                                      Map<String, Inject> values,
                                      Map<String, Extract<?>> returning) {
        String sql = Java.format(INSERT,
                schema,
                table,
                values.map(t -> t._1).mkString(SQL.COMMA),
                Java.repeat(SQL.PARAM, SQL.COMMA, values.size()),
                SQL.RETURNING + returning.map(t -> t._1).mkString(SQL.COMMA));

        return jdbc.query(sql,
                ps -> Inject.inject(ps, 1, values),
                rs -> Extract.extract(rs, 1, returning))
            .get();
    }

    @Override
    public void merge(String table, Map<String, Inject> primaryKey, Map<String, Inject> values) {
        var    insert = primaryKey.toList().appendAll(values);
        String sql    = Java.format(MERGE,
                schema,
                table,
                insert.map(t -> t._1).mkString(SQL.COMMA),
                Java.repeat(SQL.PARAM, SQL.COMMA, insert.size()),
                primaryKey.map(t -> t._1).mkString(SQL.COMMA),
                values.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.COMMA),
                "");

        jdbc.execute(sql, ps -> Inject.inject(ps, 1, insert.appendAll(values)));
    }

    @Override
    public Map<String, Object> merge(String table,
                                     Map<String, Inject> primaryKey,
                                     Map<String, Inject> values,
                                     Map<String, Extract<?>> returning) {
        var    insert = primaryKey.toList().appendAll(values);
        String sql    = Java.format(MERGE,
                schema,
                table,
                insert.map(t -> t._1).mkString(SQL.COMMA),
                Java.repeat(SQL.PARAM, SQL.COMMA, insert.size()),
                primaryKey.map(t -> t._1).mkString(SQL.COMMA),
                values.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.COMMA),
                SQL.RETURNING + returning.map(t -> t._1).mkString(SQL.COMMA));

        return jdbc.query(sql,
                ps -> Inject.inject(ps, 1, insert.appendAll(values)),
                rs -> Extract.extract(rs, 1, returning))
            .get();
    }

    @Override
    public boolean update(String table,
                          Map<String, Inject> primaryKey,
                          Map<String, Inject> values) {
        String sql = Java.format(UPDATE,
                schema,
                table,
                values.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.COMMA),
                primaryKey.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.AND),
                "");

        return 0 != jdbc.update(sql, ps -> Inject.inject(ps, 1, values.toList().appendAll(primaryKey)));
    }

    @Override
    public Option<Map<String, Object>> update(String table,
                                              Map<String, Inject> primaryKey,
                                              Map<String, Inject> values,
                                              Map<String, Extract<?>> returning) {
        String sql = Java.format(UPDATE,
                schema,
                table,
                values.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.COMMA),
                primaryKey.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.AND),
                SQL.RETURNING + returning.map(t -> t._1).mkString(SQL.COMMA));

        return jdbc.query(sql,
                ps -> Inject.inject(ps, 1, values.toList().appendAll(primaryKey)),
                rs -> Extract.extract(rs, 1, returning))
            .peekOption();
    }

    @Override
    public boolean delete(String table, Map<String, Inject> primaryKey) {
        String sql = Java.format(DELETE,
                schema,
                table,
                primaryKey.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.AND),
                "");

        return 0 != jdbc.update(sql, ps -> Inject.inject(ps, 1, primaryKey));
    }

    @Override
    public Option<Map<String, Object>> delete(String table,
                                              Map<String, Inject> primaryKey,
                                              Map<String, Extract<?>> returning) {
        String sql = Java.format(DELETE,
                schema,
                table,
                primaryKey.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.AND),
                SQL.RETURNING + returning.map(t -> t._1).mkString(SQL.COMMA));

        return jdbc.query(sql,
                ps -> Inject.inject(ps, 1, primaryKey),
                rs -> Extract.extract(rs, 1, returning))
            .peekOption();
    }

    @Override
    public Option<Map<String, Object>> select(String table,
                                              Map<String, Inject> primaryKey,
                                              Map<String, Extract<?>> returning) {
        String sql = Java.format(SELECT,
                schema,
                table,
                primaryKey.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.AND),
                returning.map(t -> t._1).mkString(SQL.COMMA));

        return jdbc.query(sql,
                ps -> Inject.inject(ps, 1, primaryKey),
                rs -> Extract.extract(rs, 1, returning))
            .peekOption();
    }
}
