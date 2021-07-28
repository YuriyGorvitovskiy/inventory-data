package org.statemach.db.sql.postgres;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.DataType;
import org.statemach.db.schema.TableInfo;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public interface TestData {
    static final Long FIRST_ROW_1_ID = 1L;
    static final Long FIRST_ROW_2_ID = 2L;
    static final Long FIRST_ROW_3_ID = 3L;

    static final Long FIRST_ROW_4_ID = 4L;
    static final Long FIRST_ROW_5_ID = 5L;
    static final Long FIRST_ROW_6_ID = 6L;
    static final Long FIRST_ROW_7_ID = 7L;
    static final Long FIRST_ROW_8_ID = 8L;
    static final Long FIRST_ROW_9_ID = 9L;

    static final UUID SECOND_ROW_1_ID = UUID.randomUUID();
    static final UUID SECOND_ROW_2_ID = UUID.randomUUID();
    static final UUID SECOND_ROW_3_ID = UUID.randomUUID();

    static final UUID SECOND_ROW_4_ID = UUID.randomUUID();
    static final UUID SECOND_ROW_5_ID = UUID.randomUUID();
    static final UUID SECOND_ROW_6_ID = UUID.randomUUID();
    static final UUID SECOND_ROW_7_ID = UUID.randomUUID();
    static final UUID SECOND_ROW_8_ID = UUID.randomUUID();
    static final UUID SECOND_ROW_9_ID = UUID.randomUUID();

    static final String THIRD_ROW_1_NAME = "Name1";
    static final String THIRD_ROW_2_NAME = "Name2";
    static final String THIRD_ROW_3_NAME = "Name3";

    static final String THIRD_ROW_4_NAME = "Name4";
    static final String THIRD_ROW_5_NAME = "Name5";
    static final String THIRD_ROW_6_NAME = "Name6";
    static final String THIRD_ROW_7_NAME = "Name7";
    static final String THIRD_ROW_8_NAME = "Name8";
    static final String THIRD_ROW_9_NAME = "Name9";

    static final Integer THIRD_ROW_1_INDX = 0;
    static final Integer THIRD_ROW_2_INDX = 1;
    static final Integer THIRD_ROW_3_INDX = 2;

    static final Integer THIRD_ROW_4_INDX = 4;
    static final Integer THIRD_ROW_5_INDX = 5;
    static final Integer THIRD_ROW_6_INDX = 6;
    static final Integer THIRD_ROW_7_INDX = 7;
    static final Integer THIRD_ROW_8_INDX = 8;
    static final Integer THIRD_ROW_9_INDX = 9;

    static final String FIRST_ROW_1_FIXED = "Fixed 1                                                                                                                                                                                                                                                         ";
    static final String FIRST_ROW_2_FIXED = "Fixed 2                                                                                                                                                                                                                                                         ";
    static final String FIRST_ROW_3_FIXED = null;

    static final String FIRST_ROW_1_VARYING = "Varying 1";
    static final String FIRST_ROW_2_VARYING = "Varying 2";
    static final String FIRST_ROW_3_VARYING = null;

    static final String FIRST_ROW_1_UNLIMIT = "Unlimited 1";
    static final String FIRST_ROW_2_UNLIMIT = "Unlimited 2";
    static final String FIRST_ROW_3_UNLIMIT = null;

    static final Double SECOND_ROW_1_DOUBLE = 1.2;
    static final Double SECOND_ROW_2_DOUBLE = 3.4;
    static final Double SECOND_ROW_3_DOUBLE = null;

    static final Integer SECOND_ROW_1_INT = 111;
    static final Integer SECOND_ROW_2_INT = 222;
    static final Integer SECOND_ROW_3_INT = null;

    static final Integer SECOND_ROW_1_SHORT = 11;
    static final Integer SECOND_ROW_2_SHORT = 22;
    static final Integer SECOND_ROW_3_SHORT = null;

    static final Long SECOND_ROW_1_LONG = 11111L;
    static final Long SECOND_ROW_2_LONG = 22222L;
    static final Long SECOND_ROW_3_LONG = null;

    static final Boolean THIRD_ROW_1_BOOL = Boolean.TRUE;
    static final Boolean THIRD_ROW_2_BOOL = Boolean.FALSE;
    static final Boolean THIRD_ROW_3_BOOL = null;

    static final Instant THIRD_ROW_1_TIME = Instant.ofEpochMilli(0);
    static final Instant THIRD_ROW_2_TIME = Instant.ofEpochMilli(System.currentTimeMillis());
    static final Instant THIRD_ROW_3_TIME = null;

    static final Map<String, Object> FIRST_ROW_1_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, FIRST_ROW_1_ID));

    static final Map<String, Object> FIRST_ROW_2_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, FIRST_ROW_2_ID));

    static final Map<String, Object> FIRST_ROW_3_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, FIRST_ROW_3_ID));

    static final Map<String, Object> FIRST_ROW_4_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, FIRST_ROW_4_ID));

    static final Map<String, Object> FIRST_ROW_1_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, FIRST_ROW_1_FIXED),
            new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, FIRST_ROW_1_VARYING),
            new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, FIRST_ROW_1_UNLIMIT));

    static final Map<String, Object> FIRST_ROW_2_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, FIRST_ROW_2_FIXED),
            new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, FIRST_ROW_2_VARYING),
            new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, FIRST_ROW_2_UNLIMIT));

    static final Map<String, Object> FIRST_ROW_3_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, FIRST_ROW_3_FIXED),
            new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, FIRST_ROW_3_VARYING),
            new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, FIRST_ROW_3_UNLIMIT));

    static final Map<String, Object> FIRST_ROW_1_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, SECOND_ROW_1_ID),
            new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, THIRD_ROW_1_NAME),
            new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, THIRD_ROW_1_INDX));

    static final Map<String, Object> FIRST_ROW_2_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, SECOND_ROW_1_ID),
            new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, THIRD_ROW_1_NAME),
            new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, THIRD_ROW_1_INDX));

    static final Map<String, Object> FIRST_ROW_3_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, null),
            new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
            new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null));

    static final Map<String, Object> SECOND_ROW_1_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, SECOND_ROW_1_ID));

    static final Map<String, Object> SECOND_ROW_2_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, SECOND_ROW_2_ID));

    static final Map<String, Object> SECOND_ROW_3_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, SECOND_ROW_3_ID));

    static final Map<String, Object> SECOND_ROW_4_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, SECOND_ROW_4_ID));

    static final Map<String, Object> SECOND_ROW_1_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, SECOND_ROW_1_DOUBLE),
            new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, SECOND_ROW_1_INT),
            new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, SECOND_ROW_1_SHORT),
            new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, SECOND_ROW_1_LONG));

    static final Map<String, Object> SECOND_ROW_2_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, SECOND_ROW_2_DOUBLE),
            new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, SECOND_ROW_2_INT),
            new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, SECOND_ROW_2_SHORT),
            new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, SECOND_ROW_2_LONG));

    static final Map<String, Object> SECOND_ROW_3_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, SECOND_ROW_3_DOUBLE),
            new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, SECOND_ROW_3_INT),
            new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, SECOND_ROW_3_SHORT),
            new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, SECOND_ROW_3_LONG));

    static final Map<String, Object> SECOND_ROW_1_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, FIRST_ROW_1_ID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, SECOND_ROW_1_ID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, SECOND_ROW_2_ID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, THIRD_ROW_1_NAME),
            new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, THIRD_ROW_1_INDX));

    static final Map<String, Object> SECOND_ROW_2_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, FIRST_ROW_1_ID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, SECOND_ROW_2_ID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, SECOND_ROW_3_ID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, THIRD_ROW_1_NAME),
            new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, THIRD_ROW_1_INDX));

    static final Map<String, Object> SECOND_ROW_3_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, null),
            new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, SECOND_ROW_3_ID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, null),
            new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, null),
            new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, null));

    static final Map<String, Object> THIRD_ROW_1_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, THIRD_ROW_1_NAME),
            new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, THIRD_ROW_1_INDX));

    static final Map<String, Object> THIRD_ROW_2_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, THIRD_ROW_2_NAME),
            new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, THIRD_ROW_2_INDX));

    static final Map<String, Object> THIRD_ROW_3_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, THIRD_ROW_3_NAME),
            new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, THIRD_ROW_3_INDX));

    static final Map<String, Object> THIRD_ROW_4_PK = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, THIRD_ROW_4_NAME),
            new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, THIRD_ROW_4_INDX));

    static final Map<String, Object> THIRD_ROW_1_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, THIRD_ROW_1_BOOL),
            new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, THIRD_ROW_1_TIME));

    static final Map<String, Object> THIRD_ROW_2_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, THIRD_ROW_2_BOOL),
            new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, THIRD_ROW_2_TIME));

    static final Map<String, Object> THIRD_ROW_3_VAL = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, THIRD_ROW_3_BOOL),
            new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, THIRD_ROW_3_TIME));

    static final Map<String, Object> THIRD_ROW_1_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, FIRST_ROW_1_ID),
            new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, SECOND_ROW_1_ID));

    static final Map<String, Object> THIRD_ROW_2_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, FIRST_ROW_1_ID),
            new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, SECOND_ROW_1_ID));

    static final Map<String, Object> THIRD_ROW_3_REF = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, null),
            new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null));

    static final Map<DataType, Function<?, Inject>> INGECTORS = List.of(
            new Tuple2<>(PostgresDataType.BIGINT, Inject.LONG),
            new Tuple2<>(PostgresDataType.BOOLEAN, Inject.BOOLEAN),
            new Tuple2<>(PostgresDataType.CHARACTER, Inject.STRING),
            new Tuple2<>(PostgresDataType.CHARACTER_VARYING, Inject.STRING),
            new Tuple2<>(PostgresDataType.DOUBLE_PRECISION, Inject.DOUBLE),
            new Tuple2<>(PostgresDataType.INTEGER, Inject.INTEGER),
            new Tuple2<>(PostgresDataType.NAME, Inject.STRING),
            new Tuple2<>(PostgresDataType.SMALLINT, Inject.INTEGER),
            new Tuple2<>(PostgresDataType.TEXT, Inject.STRING),
            new Tuple2<>(PostgresDataType.TIMESTAMP_WITH_TIME_ZONE, Inject.INSTANT_AS_TIMESTAMP),
            new Tuple2<>(PostgresDataType.UUID, Inject.UUID_AS_OBJECT))
        .toMap(t -> t);

    static final Map<String, Extract<?>> FIRST_EXTRACT = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, Extract.LONG),
            new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, Extract.OBJECT_AS_UUID),
            new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, Extract.STRING),
            new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, Extract.INTEGER),
            new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, Extract.STRING),
            new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, Extract.STRING),
            new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, Extract.STRING));

    static final Map<String, Extract<?>> SECOND_EXTRACT = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, Extract.OBJECT_AS_UUID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, Extract.LONG),
            new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, Extract.OBJECT_AS_UUID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, Extract.OBJECT_AS_UUID),
            new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, Extract.STRING),
            new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, Extract.INTEGER),
            new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, Extract.DOUBLE),
            new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, Extract.INTEGER),
            new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, Extract.INTEGER),
            new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, Extract.LONG));

    static final Map<String, Extract<?>> THIRD_EXTRACT = HashMap.ofEntries(
            new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, Extract.STRING),
            new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, Extract.INTEGER),
            new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, Extract.LONG),
            new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, Extract.OBJECT_AS_UUID),
            new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, Extract.BOOLEAN),
            new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, Extract.TIMESTAMP_AS_INSTANT));

    static Map<String, Inject> toInject(TableInfo table, Map<String, Object> values) {
        return values.map((c, v) -> new Tuple2<>(c,
                Inject.of(TestData.INGECTORS.get(table.columns.get(c).get().type).get(), v)));
    }

    static Map<String, Inject> pkToInject(TableInfo table, Map<String, Object> values) {
        return toInject(table, table.primary.get().columns.map(c -> new Tuple2<>(c, values.get(c).get())).toMap(t -> t));
    }

}
