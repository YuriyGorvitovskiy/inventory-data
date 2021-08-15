package org.statemach.db.graphql;

import java.util.Objects;

import org.statemach.db.schema.DataType;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.SQLBuilder;
import org.statemach.db.sql.Select;
import org.statemach.db.sql.postgres.PostgresDataType;
import org.statemach.util.Java;

import io.vavr.collection.List;
import io.vavr.collection.Seq;

public class Filter {

    public static enum Operator {
        NULL_CHECK,
        EQUAL,
        IN_PARAM,
        IN_TABLE,
        TEXT_SEARCH,
    }

    static final int PARAM_LIMIT = 7;

    final List<String> path;
    final boolean      plural;
    final DataType     dataType;
    final boolean      acceptNull;
    final Operator     operator;
    final Seq<?>       notNullValues;

    Filter(List<String> path,
           boolean plural,
           DataType dataType,
           boolean acceptNull,
           Operator operator,
           Seq<?> notNullValues) {
        this.path = path;
        this.plural = plural;
        this.dataType = dataType;
        this.acceptNull = acceptNull;
        this.operator = operator;
        this.notNullValues = notNullValues;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, plural, dataType, acceptNull, operator, notNullValues);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this,
                other,
                t -> t.path,
                t -> t.plural,
                t -> t.dataType,
                t -> t.acceptNull,
                t -> t.operator,
                t -> t.notNullValues);
    }

    @Override
    public String toString() {
        return "Filter@{op: " + operator +
                ", type: " + (plural ? "*" : "") + dataType +
                ", values: " + notNullValues +
                (acceptNull ? " accepts NULL" : "") +
                "}";
    }

    public static Filter of(List<String> path, boolean plural, DataType dataType, java.util.List<?> values) {
        boolean acceptNull    = values.contains(null);
        Seq<?>  notNullValues = List.ofAll(values.stream().filter(v -> v != null));
        int     valuesCount   = notNullValues.size();

        if (PostgresDataType.TSVECTOR == dataType) {
            return new Filter(
                    path,
                    plural,
                    dataType,
                    false,
                    Operator.TEXT_SEARCH,
                    notNullValues);
        }

        if (0 == valuesCount) {
            return new Filter(
                    path,
                    plural,
                    dataType,
                    acceptNull,
                    Operator.NULL_CHECK,
                    notNullValues);
        }

        if (1 == valuesCount) {
            return new Filter(
                    path,
                    plural,
                    dataType,
                    acceptNull,
                    Operator.EQUAL,
                    notNullValues);
        }

        if (PARAM_LIMIT > valuesCount) {
            return new Filter(
                    path,
                    plural,
                    dataType,
                    acceptNull,
                    Operator.IN_PARAM,
                    notNullValues);
        }

        return new Filter(
                path,
                plural,
                dataType,
                acceptNull,
                Operator.IN_TABLE,
                notNullValues);
    }

    public Condition buildCondition(GraphQLMapping mapping, SQLBuilder builder, String tableAlias) {
        var columnAlias = Select.of(tableAlias, path.last(), null);
        var injector    = mapping.injector(dataType);
        if (Operator.EQUAL == operator) {
            Condition condition = builder.equal(columnAlias, injector.apply(notNullValues.get()));
            return acceptNull ? builder.or(builder.isNull(columnAlias), condition) : condition;
        }
        if (Operator.IN_PARAM == operator) {
            Condition condition = builder.in(columnAlias, notNullValues.map(injector::apply));
            return acceptNull ? builder.or(builder.isNull(columnAlias), condition) : condition;
        }
        if (Operator.IN_TABLE == operator) {
            Condition condition = builder.inArray(columnAlias, dataType, notNullValues);
            return acceptNull ? builder.or(builder.isNull(columnAlias), condition) : condition;
        }
        if (Operator.NULL_CHECK == operator) {
            return acceptNull ? builder.isNull(columnAlias) : builder.isNotNull(columnAlias);
        }
        if (Operator.TEXT_SEARCH == operator) {
            return builder.textSearch(columnAlias, notNullValues.map(Object::toString));
        }
        return null;
    }

}
