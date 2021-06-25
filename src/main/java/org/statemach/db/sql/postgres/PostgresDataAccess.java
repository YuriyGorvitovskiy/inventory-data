package org.statemach.db.sql.postgres;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.jdbc.JDBC;
import org.statemach.db.sql.DataAccess;
import org.statemach.util.Java;

import com.yg.inventory.data.db.SQL;
import com.yg.util.DB;

import io.vavr.collection.Map;

public class PostgresDataAccess implements DataAccess {

    static final String INSERT = Java.resource("Insert.sql");
    static final String MERGE  = Java.resource("Merge.sql");
    static final String UPDATE = Java.resource("Update.sql");
    static final String DELETE = Java.resource("Delete.sql");

    public final JDBC   jdbc;
    public final String schema;

    public PostgresDataAccess(JDBC jdbc, String schema) {
        this.jdbc = jdbc;
        this.schema = schema;
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
                returning.map(t -> t._1).mkString(SQL.COMMA));

        return jdbc.query(sql,
                ps -> Inject.inject(ps, 1, values),
                rs -> Extract.extract(rs, 1, returning))
            .get();

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
                returning.map(t -> t._1).mkString(SQL.COMMA));

        return DB.query(sql,
                ps -> Inject.inject(ps, 1, insert.appendAll(values)),
                rs -> Extract.extract(rs, 1, returning))
            .get();
    }

    @Override
    public Map<String, Object> update(String table,
                                      Map<String, Inject> primaryKey,
                                      Map<String, Inject> values,
                                      Map<String, Extract<?>> returning) {
        String sql = Java.format(UPDATE,
                schema,
                table,
                values.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.COMMA),
                primaryKey.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.AND),
                returning.map(t -> t._1).mkString(SQL.COMMA));

        return DB.query(sql,
                ps -> Inject.inject(ps, 1, values.toList().appendAll(primaryKey)),
                rs -> Extract.extract(rs, 1, returning))
            .get();
    }

    @Override
    public Map<String, Object> delete(String table,
                                      Map<String, Inject> primaryKey,
                                      Map<String, Extract<?>> returning) {
        String sql = Java.format(DELETE,
                schema,
                table,
                primaryKey.map(t -> t._1 + SQL.EQUAL + SQL.PARAM).mkString(SQL.AND),
                returning.map(t -> t._1).mkString(SQL.COMMA));

        return DB.query(sql,
                ps -> Inject.inject(ps, 1, primaryKey),
                rs -> Extract.extract(rs, 1, returning))
            .get();
    }
}
