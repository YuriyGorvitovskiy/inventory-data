package org.statemach.db.sql;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public interface DataAccess {

    SQLBuilder builder();

    void insert(String table, Map<String, Inject> values);

    void merge(String table, Map<String, Inject> primaryKey, Map<String, Inject> values);

    boolean update(String table, Map<String, Inject> primaryKey, Map<String, Inject> values);

    boolean delete(String table, Map<String, Inject> primaryKey);

    Map<String, Object> insert(String table,
                               Map<String, Inject> values,
                               Map<String, Extract<?>> returning);

    Map<String, Object> merge(String table,
                              Map<String, Inject> primaryKey,
                              Map<String, Inject> values,
                              Map<String, Extract<?>> returning);

    Option<Map<String, Object>> update(String table,
                                       Map<String, Inject> primaryKey,
                                       Map<String, Inject> values,
                                       Map<String, Extract<?>> returning);

    Option<Map<String, Object>> delete(String table,
                                       Map<String, Inject> primaryKey,
                                       Map<String, Extract<?>> returning);

    Option<Map<String, Object>> select(String table,
                                       Map<String, Inject> primaryKey,
                                       Map<String, Extract<?>> returning);

    List<Map<String, Object>> query(List<View<String>> commonTableExpressions,
                                    View<Tuple2<String, Extract<?>>> query);
}
