package org.statemach.db.graphql;

import java.util.function.Function;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.jdbc.Vendor;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.DataType;
import org.statemach.db.schema.TableInfo;
import org.statemach.db.sql.postgres.PostgresDataType;
import org.statemach.util.Java;

import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public class GraphQLMapping {

    static final Map<DataType, Extract<?>> POSTGRES_EXTRACTS = HashMap.ofEntries(
            new Tuple2<>(PostgresDataType.BIGINT, Extract.LONG_AS_STRING),
            new Tuple2<>(PostgresDataType.BOOLEAN, Extract.BOOLEAN),
            new Tuple2<>(PostgresDataType.CHARACTER, Extract.STRING),
            new Tuple2<>(PostgresDataType.CHARACTER_VARYING, Extract.STRING),
            new Tuple2<>(PostgresDataType.DOUBLE_PRECISION, Extract.DOUBLE),
            new Tuple2<>(PostgresDataType.INTEGER, Extract.INTEGER),
            new Tuple2<>(PostgresDataType.NAME, Extract.STRING),
            new Tuple2<>(PostgresDataType.SMALLINT, Extract.INTEGER),
            new Tuple2<>(PostgresDataType.TEXT, Extract.STRING),
            new Tuple2<>(PostgresDataType.TIMESTAMP_WITH_TIME_ZONE, Extract.TIMESTAMP_AS_ISO8601),
            new Tuple2<>(PostgresDataType.UUID, Extract.OBJECT_AS_UUID_STRING));

    static final Map<DataType, Function<Object, Inject>> POSTGRES_INJECTORS = HashMap.ofEntries(
            new Tuple2<>(PostgresDataType.BIGINT, Java.asString(Inject.STRING_AS_LONG)),
            new Tuple2<>(PostgresDataType.BOOLEAN, Java.asBoolean(Inject.BOOLEAN)),
            new Tuple2<>(PostgresDataType.CHARACTER, Java.asString(Inject.STRING)),
            new Tuple2<>(PostgresDataType.CHARACTER_VARYING, Java.asString(Inject.STRING)),
            new Tuple2<>(PostgresDataType.DOUBLE_PRECISION, Java.asNumber(Inject.DOUBLE)),
            new Tuple2<>(PostgresDataType.INTEGER, Java.asNumber(Inject.INTEGER)),
            new Tuple2<>(PostgresDataType.NAME, Java.asString(Inject.STRING)),
            new Tuple2<>(PostgresDataType.SMALLINT, Java.asNumber(Inject.INTEGER)),
            new Tuple2<>(PostgresDataType.TEXT, Java.asString(Inject.STRING)),
            new Tuple2<>(PostgresDataType.TIMESTAMP_WITH_TIME_ZONE, Java.asString(Inject.ISO8601_AS_TIMESTAMP)),
            new Tuple2<>(PostgresDataType.TSVECTOR, Java.asString(Inject.STRING)),
            new Tuple2<>(PostgresDataType.UUID, Java.asString(Inject.STRING_AS_UUID_OBJECT)));

    static final Map<DataType, GraphQLScalarType> POSTGRES_TO_SCALAR = HashMap.ofEntries(
            new Tuple2<>(PostgresDataType.BIGINT, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.BOOLEAN, Scalars.GraphQLBoolean),
            new Tuple2<>(PostgresDataType.CHARACTER, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.CHARACTER_VARYING, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.DOUBLE_PRECISION, Scalars.GraphQLFloat),
            new Tuple2<>(PostgresDataType.INTEGER, Scalars.GraphQLInt),
            new Tuple2<>(PostgresDataType.NAME, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.SMALLINT, Scalars.GraphQLInt),
            new Tuple2<>(PostgresDataType.TEXT, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.TIMESTAMP_WITH_TIME_ZONE, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.TSVECTOR, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.UUID, Scalars.GraphQLString));

    final Map<DataType, Extract<?>>               extracts;
    final Map<DataType, Function<Object, Inject>> injectors;
    final Map<DataType, GraphQLScalarType>        scalars;

    GraphQLMapping(Map<DataType, Extract<?>> extracts,
                   Map<DataType, Function<Object, Inject>> injectors,
                   Map<DataType, GraphQLScalarType> scalars) {
        this.extracts = extracts;
        this.injectors = injectors;
        this.scalars = scalars;
    }

    public static GraphQLMapping of(Vendor vendor) {
        switch (vendor) {
            case POSTGRES:
                return new GraphQLMapping(POSTGRES_EXTRACTS, POSTGRES_INJECTORS, POSTGRES_TO_SCALAR);
        }
        throw new RuntimeException("Vendor " + vendor + " is not supported");
    }

    public Function<Object, Inject> injector(DataType type) {
        return injectors.get(type).get();
    }

    public GraphQLScalarType scalar(DataType type) {
        return scalars.get(type).get();
    }

    public GraphQLScalarType scalar(TableInfo table, ColumnInfo column) {
        GraphQLScalarType type = scalar(column.type);
        if (Scalars.GraphQLString != type) {
            return type;
        }
        if (table.primary.isDefined()
                && 1 == table.primary.get().columns.size()
                && table.primary.get().columns.contains(column.name)) {
            return Scalars.GraphQLID;
        }
        if (table.outgoing.values()
            .exists(o -> 1 == o.matchingColumns.size()
                    && column.name.equals(o.matchingColumns.get().from))) {
            return Scalars.GraphQLID;
        }
        if (table.incoming.values()
            .exists(o -> 1 == o.matchingColumns.size()
                    && column.name.equals(o.matchingColumns.get().to))) {
            return Scalars.GraphQLID;
        }

        return scalar(column.type);
    }

    public Extract<?> extract(DataType type) {
        return extracts.get(type).get();
    }

    public boolean isExtractable(DataType type) {
        return extracts.containsKey(type);
    }

    public boolean isFilterable(DataType type) {
        return injectors.containsKey(type);
    }

    public boolean isMutable(DataType type) {
        return extracts.containsKey(type);
    }

}
