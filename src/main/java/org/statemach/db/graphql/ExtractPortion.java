package org.statemach.db.graphql;

import java.util.Objects;

import org.statemach.util.Java;

import io.vavr.collection.List;

public class ExtractPortion {
    public static final ExtractPortion EMPTY = new ExtractPortion(List.empty(), List.empty(), List.empty());

    public final List<ForeignKeyPath> keys;
    public final List<ExtractValue>   values;
    public final List<SubQuery>       queries;

    public static ExtractPortion ofKey(List<String> path, List<ExtractValue> values) {
        return new ExtractPortion(List.of(new ForeignKeyPath(path, values)), values, List.empty());
    }

    public static ExtractPortion ofValue(ExtractValue value) {
        return new ExtractPortion(List.empty(), List.of(value), List.empty());
    }

    public static ExtractPortion ofValues(List<ExtractValue> values) {
        return new ExtractPortion(List.empty(), values, List.empty());
    }

    public static ExtractPortion ofQuery(SubQuery query) {
        return new ExtractPortion(List.empty(), query.extracts, List.of(query));
    }

    ExtractPortion(List<ForeignKeyPath> keys, List<ExtractValue> values, List<SubQuery> queries) {
        this.keys = keys;
        this.values = values;
        this.queries = queries;
    }

    public ExtractPortion append(ExtractPortion other) {
        return new ExtractPortion(keys.appendAll(other.keys),
                values.appendAll(other.values),
                queries.appendAll(other.queries));
    }

    public ExtractPortion distinctValues() {
        return new ExtractPortion(keys, values.distinctBy(v -> v.name), queries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keys, values, queries);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.keys, t -> t.values, t -> t.queries);
    }

    @Override
    public String toString() {
        return "ExtractPortion@{keys: " + keys +
                ", values: " + values +
                ", queries: " + queries +
                "}";
    }

}
