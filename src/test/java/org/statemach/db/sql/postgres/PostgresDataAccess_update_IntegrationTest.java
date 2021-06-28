package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.TableInfo;
import org.statemach.util.Json;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.control.Option;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresDataAccess_update_IntegrationTest {

    final PostgresDataAccess subject = new PostgresDataAccess(TestDB.jdbc, TestDB.schema);

    @BeforeAll
    static void setup() {
        TestDB.setup();
        TestDB.truncateAll();
        TestDB.insertAll();
    }

    @AfterEach
    void restore() {
        TestDB.updateAllTablesRow1();
    }

    @Test
    void update_first_non_existing() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, TestData.FIRST_ROW_2_FIXED));

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.FIRST_ROW_4_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final boolean success = subject.update(table.name, PK, INJECT);

        // Verify
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT);
        assertFalse(success);
        assertFalse(result.isDefined());
    }

    @Test
    void update_first_non_existing_returning() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, TestData.FIRST_ROW_2_FIXED));

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.FIRST_ROW_4_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final Option<Map<String, Object>> result = subject.update(table.name, PK, INJECT, TestData.FIRST_EXTRACT);

        // Verify
        final Option<Map<String, Object>> result1 = subject.select(table.name, PK, TestData.FIRST_EXTRACT);
        assertFalse(result.isDefined());
        assertFalse(result1.isDefined());
    }

    @Test
    void update_first_single_column() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, TestData.FIRST_ROW_2_FIXED));

        final Map<String, Object> ROW = UPDATE
            .merge(TestData.FIRST_ROW_1_PK)
            .merge(TestData.FIRST_ROW_1_VAL)
            .merge(TestData.FIRST_ROW_1_REF);

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.FIRST_ROW_1_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        boolean success = subject.update(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertTrue(success);
        assertEquals(ROW, result);
    }

    @Test
    void update_first_single_column_returning() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, TestData.FIRST_ROW_2_VARYING));

        final Map<String, Object> ROW = UPDATE
            .merge(TestData.FIRST_ROW_1_PK)
            .merge(TestData.FIRST_ROW_1_VAL)
            .merge(TestData.FIRST_ROW_1_REF);

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.FIRST_ROW_1_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.FIRST_EXTRACT).get();

        // Verify
        final Map<String, Object> result2 = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_first_nulls() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.update(table.name, PK, INJECT);

        // Verify
        final Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void update_first_null_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.FIRST_EXTRACT).get();

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_first_all() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, TestData.SECOND_ROW_2_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, TestData.THIRD_ROW_2_NAME),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, TestData.THIRD_ROW_2_INDX),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, TestData.FIRST_ROW_2_FIXED),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, TestData.FIRST_ROW_2_VARYING),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, TestData.FIRST_ROW_2_UNLIMIT));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.update(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void update_first_all_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, TestData.SECOND_ROW_3_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, TestData.THIRD_ROW_3_NAME),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, TestData.THIRD_ROW_3_INDX),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, TestData.FIRST_ROW_3_FIXED),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, TestData.FIRST_ROW_3_VARYING),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, TestData.FIRST_ROW_3_UNLIMIT));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.FIRST_EXTRACT).get();

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_second_non_existing() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, TestData.SECOND_ROW_2_DOUBLE));

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.SECOND_ROW_4_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final boolean success = subject.update(table.name, PK, INJECT);

        // Verify
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT);
        assertFalse(success);
        assertFalse(result.isDefined());
    }

    @Test
    void update_second_non_existing_returning() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, TestData.SECOND_ROW_2_DOUBLE));

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.SECOND_ROW_4_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final Option<Map<String, Object>> result = subject.update(table.name, PK, INJECT, TestData.SECOND_EXTRACT);

        // Verify
        final Option<Map<String, Object>> result1 = subject.select(table.name, PK, TestData.SECOND_EXTRACT);
        assertFalse(result.isDefined());
        assertFalse(result1.isDefined());
    }

    @Test
    void update_second_single_column() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, TestData.SECOND_ROW_2_DOUBLE));

        final Map<String, Object> ROW = UPDATE
            .merge(TestData.SECOND_ROW_1_PK)
            .merge(TestData.SECOND_ROW_1_VAL)
            .merge(TestData.SECOND_ROW_1_REF);

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.SECOND_ROW_1_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        subject.update(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void update_second_single_column_returning() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, TestData.SECOND_ROW_2_DOUBLE));

        final Map<String, Object> ROW = UPDATE
            .merge(TestData.SECOND_ROW_1_PK)
            .merge(TestData.SECOND_ROW_1_VAL)
            .merge(TestData.SECOND_ROW_1_REF);

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.SECOND_ROW_1_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.SECOND_EXTRACT).get();

        // Verify
        final Map<String, Object> result2 = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_second_nulls() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW)
            .removeAll(PK.keySet())
            .put(TestSchema.COLUMN_SECOND_TWO.name, Inject.STRING_AS_UUID_OBJECT.apply(null))
            .put(TestSchema.COLUMN_SECOND_DOUBLE.name, Inject.STRING_AS_DOUBLE.apply(null))
            .put(TestSchema.COLUMN_SECOND_INT.name, Inject.STRING_AS_INTEGER.apply(null))
            .put(TestSchema.COLUMN_SECOND_SHORT.name, Inject.STRING_AS_INTEGER.apply(null))
            .put(TestSchema.COLUMN_SECOND_LONG.name, Inject.STRING_AS_LONG.apply(null));

        // Execute
        subject.update(table.name, PK, INJECT);

        // Verify
        final Map<String, Object> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void update_second_null_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.SECOND_EXTRACT).get();

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_second_all() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, TestData.FIRST_ROW_2_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, TestData.THIRD_ROW_2_NAME),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, TestData.THIRD_ROW_2_INDX),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, TestData.SECOND_ROW_2_DOUBLE),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, TestData.SECOND_ROW_2_INT),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, TestData.SECOND_ROW_2_SHORT),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, TestData.SECOND_ROW_2_LONG));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW)
            .removeAll(PK.keySet())
            .put(TestSchema.COLUMN_SECOND_TWO.name, Inject.STRING_AS_UUID_OBJECT.apply("" + TestData.SECOND_ROW_1_ID))
            .put(TestSchema.COLUMN_SECOND_DOUBLE.name, Inject.STRING_AS_DOUBLE.apply("" + TestData.SECOND_ROW_2_DOUBLE))
            .put(TestSchema.COLUMN_SECOND_INT.name, Inject.STRING_AS_INTEGER.apply("" + TestData.SECOND_ROW_2_INT))
            .put(TestSchema.COLUMN_SECOND_SHORT.name, Inject.STRING_AS_INTEGER.apply("" + TestData.SECOND_ROW_2_SHORT))
            .put(TestSchema.COLUMN_SECOND_LONG.name, Inject.STRING_AS_LONG.apply("" + TestData.SECOND_ROW_2_LONG));

        // Execute
        subject.update(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void update_second_all_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, TestData.FIRST_ROW_3_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, TestData.SECOND_ROW_3_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, TestData.SECOND_ROW_3_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, TestData.THIRD_ROW_3_NAME),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, TestData.THIRD_ROW_3_INDX),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, TestData.SECOND_ROW_3_DOUBLE),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, TestData.SECOND_ROW_3_LONG),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, TestData.SECOND_ROW_3_INT),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, TestData.SECOND_ROW_3_SHORT));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.SECOND_EXTRACT).get();

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_third_non_existing() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, TestData.THIRD_ROW_2_BOOL));

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.THIRD_ROW_4_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final boolean success = subject.update(table.name, PK, INJECT);

        // Verify
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT);
        assertFalse(success);
        assertFalse(result.isDefined());
    }

    @Test
    void update_third_non_existing_returning() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, TestData.THIRD_ROW_2_BOOL));

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.THIRD_ROW_4_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final Option<Map<String, Object>> result = subject.update(table.name, PK, INJECT, TestData.THIRD_EXTRACT);

        // Verify
        final Option<Map<String, Object>> result1 = subject.select(table.name, PK, TestData.THIRD_EXTRACT);
        assertFalse(result.isDefined());
        assertFalse(result1.isDefined());
    }

    @Test
    void update_third_single_column() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, TestData.THIRD_ROW_2_TIME));

        final Map<String, Object> ROW = UPDATE
            .merge(TestData.THIRD_ROW_1_PK)
            .merge(TestData.THIRD_ROW_1_VAL)
            .merge(TestData.THIRD_ROW_1_REF);

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.THIRD_ROW_1_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE)
            .put(TestSchema.COLUMN_THIRD_TIME.name,
                    Inject.TIMESTAMP.apply(new Timestamp(TestData.THIRD_ROW_2_TIME.toEpochMilli())));

        // Execute
        subject.update(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void update_third_single_column_returning() {
        // Setup
        final TableInfo           table  = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> UPDATE = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, TestData.THIRD_ROW_2_BOOL));

        final Map<String, Object> ROW = UPDATE
            .merge(TestData.THIRD_ROW_1_PK)
            .merge(TestData.THIRD_ROW_1_VAL)
            .merge(TestData.THIRD_ROW_1_REF);

        final Map<String, Inject> PK     = TestData.toInject(table, TestData.THIRD_ROW_1_PK);
        final Map<String, Inject> INJECT = TestData.toInject(table, UPDATE);

        // Execute
        final Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.THIRD_EXTRACT).get();

        // Verify
        final Map<String, Object> result2 = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_third_nulls() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_1_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_1_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW)
            .removeAll(PK.keySet())
            .put(TestSchema.COLUMN_THIRD_BOOL.name, Inject.STRING_AS_BOOLEAN.apply(null))
            .put(TestSchema.COLUMN_THIRD_TIME.name, Inject.ISO8601_AS_TIMESTAMP.apply(null));

        // Execute
        subject.update(table.name, PK, INJECT);

        // Verify
        final Map<String, Object> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void update_third_null_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_1_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_1_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW)
            .removeAll(PK.keySet())
            .put(TestSchema.COLUMN_THIRD_TIME.name, Inject.TIMESTAMP.apply(null));

        // Execute
        Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.THIRD_EXTRACT).get();

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_third_all() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_1_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_1_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, TestData.FIRST_ROW_2_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, TestData.SECOND_ROW_2_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, TestData.THIRD_ROW_2_BOOL),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, TestData.THIRD_ROW_2_TIME));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW)
            .removeAll(PK.keySet())
            .put(TestSchema.COLUMN_THIRD_BOOL.name,
                    Inject.STRING_AS_BOOLEAN.apply("" + TestData.THIRD_ROW_2_BOOL))
            .put(TestSchema.COLUMN_THIRD_TIME.name,
                    Inject.ISO8601_AS_TIMESTAMP.apply(Json.toISO8601(TestData.THIRD_ROW_2_TIME)));

        // Execute
        subject.update(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void update_third_all_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_1_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_1_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, TestData.FIRST_ROW_3_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, TestData.SECOND_ROW_3_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, TestData.THIRD_ROW_3_BOOL),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, TestData.THIRD_ROW_3_TIME));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.update(table.name, PK, INJECT, TestData.THIRD_EXTRACT).get();

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

}
