package org.statemach.db.graphql;

import org.statemach.db.jdbc.Vendor;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.DataType;
import org.statemach.db.schema.TableInfo;
import org.statemach.db.sql.postgres.PostgresDataType;

import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public class GraphQLMapping {

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
            new Tuple2<>(PostgresDataType.TIMESTAMP_WITH_TIMEZONE, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.TSVECTOR, Scalars.GraphQLString),
            new Tuple2<>(PostgresDataType.UUID, Scalars.GraphQLString));

    final Map<DataType, GraphQLScalarType> scalars;

    GraphQLMapping(Map<DataType, GraphQLScalarType> scalars) {
        this.scalars = scalars;
    }

    public static GraphQLMapping of(Vendor vendor) {
        if (Vendor.POSTGRES == vendor) {
            return new GraphQLMapping(POSTGRES_TO_SCALAR);
        }
        throw new RuntimeException("Vendor " + vendor + " is not supported");
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
        // Foreign Key always referencing to the Primary Key.
        // There is no need to check incoming relation
        return scalar(column.type);
    }

}
