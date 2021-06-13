package com.yg.inventory.data.graphql;

import java.util.function.Function;

import com.yg.inventory.data.db.Condition;
import com.yg.inventory.data.db.SQL;
import com.yg.util.DB;

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
    final DB.DataType  dataType;
    final boolean      acceptNull;
    final Operator     operator;
    final Seq<?>       notNullValues;

    Filter(List<String> path,
           boolean plural,
           DB.DataType dataType,
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

    public static Filter of(List<String> path, boolean plural, DB.DataType dataType, Seq<?> values) {
        boolean acceptNull    = (null == values || values.contains(null));
        Seq<?>  notNullValues = (null == values ? List.empty() : values).filter(v -> v != null);
        int     valuesCount   = notNullValues.size();

        if (DB.DataType.TEXT_SEARCH_VECTOR == dataType) {
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

    public Condition buildCondition(String tableAlias) {
        String columnAlias = tableAlias + SQL.DOT + path.last();
        switch (operator) {
            case EQUAL: {
                Function<Object, DB.Inject> injector  = DB.DATA_TYPE_INJECT.get(dataType).get();
                Condition                   condition = Condition.equal(columnAlias, injector.apply(notNullValues.get()));
                return acceptNull ? Condition.or(Condition.isNull(columnAlias), condition) : condition;
            }
            case IN_PARAM: {
                Function<Object, DB.Inject> injector  = DB.DATA_TYPE_INJECT.get(dataType).get();
                Condition                   condition = Condition.in(columnAlias, notNullValues.map(injector::apply));
                return acceptNull ? Condition.or(Condition.isNull(columnAlias), condition) : condition;
            }
            case IN_TABLE: {
                DB.Inject inject    = DB.Injects.ARRAY.apply(dataType, notNullValues);
                Condition condition = Condition.inArray(columnAlias, dataType, inject);
                return acceptNull ? Condition.or(Condition.isNull(columnAlias), condition) : condition;
            }
            case NULL_CHECK: {
                return acceptNull ? Condition.isNull(columnAlias) : Condition.isNotNull(columnAlias);
            }
            case TEXT_SEARCH: {
                return Condition.textSearch(columnAlias, notNullValues.map(Object::toString));
            }
        }
        return null;
    }

}
