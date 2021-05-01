package com.yg.inventory.data.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.yg.util.DB;
import com.yg.util.DB.Extract;
import com.yg.util.DB.Inject;
import com.yg.util.DB.Injects;
import com.yg.util.Java;

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

    public List<Map<String, Object>> queryById(String table, List<Tuple2<String, Extract<?>>> select, Long id) {
        return DB.query(Java.format(QEURY_BY_ID, select.map(t -> t._1).mkString(", "), table),
                ps -> ps.setLong(1, id),
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
                ps -> inject(ps, 1, List.of(condition, limit, skip)),
                rs -> extract(rs, select));
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
                ps -> inject(ps, 1, insert),
                rs -> extract(rs, returning));
    }

    public List<Map<String, Object>> mergeById(String table,
                                               Long id,
                                               List<Tuple2<String, Inject>> values,
                                               List<Tuple2<String, Extract<?>>> returning) {

        var idInject = new Tuple2<>(ID, Injects.LONG.apply(id));
        var insert   = List.of(idInject).appendAll(values);

        String sql = Java.format(MERGE_BY_ID,
                table,
                insert.map(t -> t._1).mkString(", "),
                Java.repeat("?", ", ", insert.size()),
                values.map(t -> t._1 + " = ?").mkString(", "),
                returning.map(t -> t._1).mkString(", "));

        return DB.query(sql,
                ps -> inject(ps, 1, insert.appendAll(values)),
                rs -> extract(rs, returning));
    }

    public List<Map<String, Object>> updateById(String table,
                                                Long id,
                                                List<Tuple2<String, Inject>> values,
                                                List<Tuple2<String, Extract<?>>> returning) {
        var    idInject = new Tuple2<>(ID, Injects.LONG.apply(id));
        String sql      = Java.format(UPDATE_BY_ID,
                table,
                values.map(t -> t._1 + " = ?").mkString(", "),
                returning.map(t -> t._1).mkString(", "));

        return DB.query(sql,
                ps -> inject(ps, 1, values.append(idInject)),
                rs -> extract(rs, returning));
    }

    public List<Map<String, Object>> deleteById(String table,
                                                Long id,
                                                List<Tuple2<String, Extract<?>>> returning) {
        String sql = Java.format(DELETE_BY_ID,
                table,
                returning.map(t -> t._1).mkString(", "));

        return DB.query(sql,
                ps -> ps.setLong(1, id),
                rs -> extract(rs, returning));
    }

    int inject(PreparedStatement ps, int index, List<Tuple2<String, Inject>> values) {
        return values.foldLeft(index, (i, t) -> Java.soft(() -> t._2.set(ps, i)));
    }

    Map<String, Object> extract(ResultSet rs, List<Tuple2<String, Extract<?>>> columns) {
        return columns.zipWithIndex()
            .toLinkedMap(t -> Java.soft(
                    () -> new Tuple2<String, Object>(
                            t._1._1,
                            t._1._2.get(rs, t._2 + 1))));
    }

}
