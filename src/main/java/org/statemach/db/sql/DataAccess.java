package org.statemach.db.sql;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;

import io.vavr.collection.Map;

public interface DataAccess {

    public Map<String, Object> insert(String table,
                                      Map<String, Inject> values,
                                      Map<String, Extract<?>> returning);

    public Map<String, Object> merge(String table,
                                     Map<String, Inject> primaryKey,
                                     Map<String, Inject> values,
                                     Map<String, Extract<?>> returning);

    public Map<String, Object> update(String table,
                                      Map<String, Inject> primaryKey,
                                      Map<String, Inject> values,
                                      Map<String, Extract<?>> returning);

    public Map<String, Object> delete(String table,
                                      Map<String, Inject> primaryKey,
                                      Map<String, Extract<?>> returning);
}
