package com.yg.inventory.data.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import org.statemach.util.Java;

import com.yg.util.DB;
import com.yg.util.DB.Extract;
import com.yg.util.DB.Inject;
import com.yg.util.DB.Injects;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class DataAccess {

    static final String ID = "id";

    final String DELETE_BY_ID       = Java.resource("DeleteById.sql");
    final String INSERT             = Java.resource("Insert.sql");
    final String MERGE_BY_ID        = Java.resource("MergeById.sql");
    final String QEURY_ALL          = Java.resource("QueryAll.sql");
    final String QEURY_BY_CONDITION = Java.resource("QueryByCondition.sql");
    final String QEURY_BY_ID        = Java.resource("QueryById.sql");
    final String UPDATE_BY_ID       = Java.resource("UpdateById.sql");

    public List<Map<String, Object>> queryById(String table, List<Tuple2<String, Extract<?>>> select, UUID id) {
        return DB.query(Java.format(QEURY_BY_ID, select.map(t -> t._1).mkString(", "), table),
                ps -> ps.setObject(1, id),
                rs -> extract(rs, select));
    }

    public List<Map<String, Object>> queryByCondition(String table,
                                                      List<Tuple2<String, Extract<?>>> select,
                                                      Tuple2<String, DB.Inject> condition,
                                                      List<Tuple2<String, Boolean>> order,
                                                      Tuple2<Long, Integer> skipLimit) {
        var skip  = new Tuple2<>("", Injects.LONG.apply(skipLimit._1));
        var limit = new Tuple2<>("", Injects.INTEGER.apply(skipLimit._2));

        String sql = Java.format(QEURY_BY_CONDITION,
                select.map(t -> t._1).mkString(", "),
                table,
                condition._1,
                order.map(t -> t._1 + (t._2 ? " ASC" : " DESC")).mkString(", "));

        return DB.query(sql,
                ps -> inject(ps, 1, List.of(condition, limit, skip).map(t -> t._2)),
                rs -> extract(rs, select));
    }

    public List<Map<String, Object>> queryByView(List<View<Void>> views, View<Extract<?>> query) {
        StringBuilder sb     = new StringBuilder();
        int           indent = 0;
        if (!views.isEmpty()) {
            indent = 1;
            sb.append(SQL.WITH);
            sb.append(views.map(v -> v.declare(1)).mkString(SQL.COMMA));
            sb.append(SQL.NEXT_LINE);
        }
        sb.append(query.sql(indent));

        List<Inject>                     injects = views.flatMap(View::injects).appendAll(query.injects());
        List<Tuple2<String, Extract<?>>> extract = query.select.map(c -> new Tuple2<>(c.name, c._1));
        return DB.query(sb.toString(),
                ps -> inject(ps, 1, injects),
                rs -> extract(rs, extract));
    }

    public List<Map<String, Object>> insert(String table,
                                            List<Tuple2<String, Inject>> insert,
                                            List<Tuple2<String, Extract<?>>> returning) {
        String sql = Java.format(INSERT,
                table,
                insert.map(t -> t._1).mkString(", "),
                Java.repeat("?", ", ", insert.size()),
                returning.map(t -> t._1).mkString(", "));

        return DB.query(sql,
                ps -> inject(ps, 1, insert.map(t -> t._2)),
                rs -> extract(rs, returning));
    }

    public List<Map<String, Object>> mergeById(String table,
                                               UUID id,
                                               List<Tuple2<String, Inject>> values,
                                               List<Tuple2<String, Extract<?>>> returning) {

        var idInject = new Tuple2<>(ID, Injects.UUID.apply(id));
        var insert   = List.of(idInject).appendAll(values);

        String sql = Java.format(MERGE_BY_ID,
                table,
                insert.map(t -> t._1).mkString(", "),
                Java.repeat("?", ", ", insert.size()),
                values.map(t -> t._1 + " = ?").mkString(", "),
                returning.map(t -> t._1).mkString(", "));

        return DB.query(sql,
                ps -> inject(ps, 1, insert.appendAll(values).map(t -> t._2)),
                rs -> extract(rs, returning));
    }

    public List<Map<String, Object>> updateById(String table,
                                                UUID id,
                                                List<Tuple2<String, Inject>> values,
                                                List<Tuple2<String, Extract<?>>> returning) {
        var    idInject = new Tuple2<>(ID, Injects.UUID.apply(id));
        String sql      = Java.format(UPDATE_BY_ID,
                table,
                values.map(t -> t._1 + " = ?").mkString(", "),
                returning.map(t -> t._1).mkString(", "));

        return DB.query(sql,
                ps -> inject(ps, 1, values.append(idInject).map(t -> t._2)),
                rs -> extract(rs, returning));
    }

    public List<Map<String, Object>> deleteById(String table,
                                                UUID id,
                                                List<Tuple2<String, Extract<?>>> returning) {
        String sql = Java.format(DELETE_BY_ID,
                table,
                returning.map(t -> t._1).mkString(", "));

        return DB.query(sql,
                ps -> ps.setObject(1, id),
                rs -> extract(rs, returning));
    }

    int inject(PreparedStatement ps, int index, List<Inject> injects) {
        return injects.foldLeft(index, (x, n) -> Java.soft(() -> n.set(ps, x)));
    }

    Map<String, Object> extract(ResultSet rs, List<Tuple2<String, Extract<?>>> columns) {
        return columns.zipWithIndex()
            .toLinkedMap(t -> Java.soft(
                    () -> new Tuple2<String, Object>(
                            t._1._1,
                            t._1._2.get(rs, t._2 + 1))));
    }
}
