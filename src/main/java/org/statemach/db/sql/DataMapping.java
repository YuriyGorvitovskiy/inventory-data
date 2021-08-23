package org.statemach.db.sql;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.DataType;

import io.vavr.control.Option;

public interface DataMapping {

    Option<Inject> stringInject(DataType type, String value);

    Option<Inject> stringArray(DataType type, Iterable<String> value);

    Option<Inject> jsonInject(DataType type, Object value);

    Option<Inject> jsonArray(DataType type, Iterable<?> value);

    Option<Extract<?>> extract(DataType type);

}
