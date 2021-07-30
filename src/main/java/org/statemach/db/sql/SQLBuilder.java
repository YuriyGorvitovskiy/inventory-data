package org.statemach.db.sql;

import org.statemach.db.jdbc.Inject;
import org.statemach.db.jdbc.Vendor;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.DataType;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Traversable;

public interface SQLBuilder {

    public Vendor getVendor();

    public default Condition and(Condition... child) {
        return and(List.of(child));
    }

    public default Condition or(Condition... child) {
        return or(List.of(child));
    }

    public Condition and(List<Condition> child);

    public Condition or(List<Condition> child);

    public Condition not(Condition child);

    public Condition isNull(Select<?> column);

    public Condition isNotNull(Select<?> column);

    public Condition equal(Select<?> column, Inject value);

    public Condition equal(Select<?> left, Select<?> right);

    public Condition in(Select<?> column, Traversable<Inject> values);

    public Condition inArray(Select<?> column, DataType elementType, Traversable<?> array);

    public Condition textSearch(Select<?> column, Traversable<String> values);

    public TableLike arrayAsTable(DataType type, List<ColumnInfo> columns, Traversable<Map<String, Object>> values);

    public TableLike arrayAsTable(ColumnInfo column, Traversable<Object> values);

}
