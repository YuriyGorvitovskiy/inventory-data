package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.TableInfo;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresDataAccess_merge_IntegrationTest {

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
    void insert_into_first_single_column() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_4_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table,
                ROW.filterKeys(k -> TestSchema.COLUMN_FIRST_SECOND.name.equals(k)));

        // Execute
        subject.merge(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_first_single_column_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_5_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table,
                ROW.filterKeys(k -> TestSchema.COLUMN_FIRST_SECOND.name.equals(k)));

        // Execute
        final Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.FIRST_EXTRACT);

        // Verify
        final Map<String, Object> result2 = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_first_nulls() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_6_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

        // Verify
        final Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_first_null_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_7_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.FIRST_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_first_all() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_8_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, TestData.THIRD_ROW_1_NAME),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, TestData.THIRD_ROW_1_INDX),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, TestData.FIRST_ROW_1_FIXED),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, TestData.FIRST_ROW_1_VARYING),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, TestData.FIRST_ROW_1_UNLIMIT));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_first_all_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_9_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, TestData.THIRD_ROW_1_NAME),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, TestData.THIRD_ROW_1_INDX),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, TestData.FIRST_ROW_1_FIXED),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, TestData.FIRST_ROW_1_VARYING),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, TestData.FIRST_ROW_1_UNLIMIT));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.FIRST_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_second_single_column() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_4_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table,
                ROW.filterKeys(k -> TestSchema.COLUMN_SECOND_FIRST.name.equals(k)));

        // Execute
        subject.merge(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_second_single_column_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_5_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table,
                ROW.filterKeys(k -> TestSchema.COLUMN_SECOND_FIRST.name.equals(k)));

        // Execute
        final Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.SECOND_EXTRACT);

        // Verify
        final Map<String, Object> result2 = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_second_nulls() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_6_ID),
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
        subject.merge(table.name, PK, INJECT);

        // Verify
        final Map<String, Object> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_second_null_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_7_ID),
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
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.SECOND_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_second_all() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_8_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, TestData.SECOND_ROW_2_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, TestData.THIRD_ROW_1_NAME),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, TestData.THIRD_ROW_1_INDX),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, TestData.SECOND_ROW_1_DOUBLE),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, TestData.SECOND_ROW_1_INT),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, TestData.SECOND_ROW_1_SHORT),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, TestData.SECOND_ROW_1_LONG));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_second_all_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_9_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, TestData.SECOND_ROW_2_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, TestData.THIRD_ROW_1_NAME),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, TestData.THIRD_ROW_1_INDX),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, TestData.SECOND_ROW_1_DOUBLE),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, TestData.SECOND_ROW_1_LONG),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, TestData.SECOND_ROW_1_INT),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, TestData.SECOND_ROW_1_SHORT));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.SECOND_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_third_single_column() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_5_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_5_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table,
                ROW.filterKeys(k -> TestSchema.COLUMN_THIRD_FIRST.name.equals(k)));

        // Execute
        subject.merge(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_third_single_column_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_6_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_6_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table,
                ROW.filterKeys(k -> TestSchema.COLUMN_THIRD_FIRST.name.equals(k)));

        // Execute
        final Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.THIRD_EXTRACT);

        // Verify
        final Map<String, Object> result2 = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_third_nulls() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_7_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_7_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

        // Verify
        final Map<String, Object> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_third_null_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_8_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_8_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, null));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.THIRD_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_third_all() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_9_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_9_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, TestData.THIRD_ROW_1_BOOL),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, TestData.THIRD_ROW_1_TIME));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_third_all_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_4_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_4_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, TestData.FIRST_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, TestData.SECOND_ROW_1_ID),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, TestData.THIRD_ROW_1_BOOL),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, TestData.THIRD_ROW_1_TIME));

        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.THIRD_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
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
        subject.merge(table.name, PK, INJECT);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
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
        final Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.FIRST_EXTRACT);

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
        subject.merge(table.name, PK, INJECT);

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
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.FIRST_EXTRACT);

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
        subject.merge(table.name, PK, INJECT);

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
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.FIRST_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
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
        subject.merge(table.name, PK, INJECT);

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
        final Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.SECOND_EXTRACT);

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
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

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
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.SECOND_EXTRACT);

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
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

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
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.SECOND_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void update_third_single_column() {
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
        subject.merge(table.name, PK, INJECT);

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
        final Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.THIRD_EXTRACT);

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
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

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
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.THIRD_EXTRACT);

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
        final Map<String, Inject> INJECT = TestData.toInject(table, ROW).removeAll(PK.keySet());

        // Execute
        subject.merge(table.name, PK, INJECT);

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
        Map<String, Object> result1 = subject.merge(table.name, PK, INJECT, TestData.THIRD_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

}
