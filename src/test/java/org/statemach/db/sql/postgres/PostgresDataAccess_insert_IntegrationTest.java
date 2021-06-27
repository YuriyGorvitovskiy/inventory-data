package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.TableInfo;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresDataAccess_insert_IntegrationTest {

    final PostgresDataAccess subject = new PostgresDataAccess(TestDB.jdbc, TestDB.schema);

    @BeforeAll
    static void setup() {
        TestDB.setup();
        TestDB.truncateAll();
        TestDB.insertAll();
    }

    @Test
    void insert_into_first_pk() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_4_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, null));

        final Map<String, Inject> PK = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, PK);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_first_pk_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, TestData.FIRST_ROW_5_ID),
                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, null),
                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, null));

        final Map<String, Inject> PK = TestData.pkToInject(table, ROW);

        // Execute
        final Map<String, Object> result1 = subject.insert(table.name, PK, TestData.FIRST_EXTRACT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, INJECT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        Map<String, Object> result1 = subject.insert(table.name, INJECT, TestData.FIRST_EXTRACT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, INJECT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        Map<String, Object> result1 = subject.insert(table.name, INJECT, TestData.FIRST_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_second_pk() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_4_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, null));

        final Map<String, Inject> PK = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, PK);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_second_pk_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_ID.name, TestData.SECOND_ROW_5_ID),
                new Tuple2<>(TestSchema.COLUMN_SECOND_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_ONE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_NAME.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_THIRD_INDX.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_DOUBLE.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_INT.name, null),
                new Tuple2<>(TestSchema.COLUMN_SECOND_SHORT.name, null));

        final Map<String, Inject> PK = TestData.pkToInject(table, ROW);

        // Execute
        final Map<String, Object> result1 = subject.insert(table.name, PK, TestData.SECOND_EXTRACT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, INJECT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        Map<String, Object> result1 = subject.insert(table.name, INJECT, TestData.SECOND_EXTRACT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, INJECT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        Map<String, Object> result1 = subject.insert(table.name, INJECT, TestData.SECOND_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

    @Test
    void insert_into_third_pk() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_5_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_5_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, null));

        final Map<String, Inject> PK = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, PK);

        // Verify
        Map<String, Object> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result);
    }

    @Test
    void insert_into_third_pk_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Object> ROW   = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_NAME.name, TestData.THIRD_ROW_6_NAME),
                new Tuple2<>(TestSchema.COLUMN_THIRD_INDX.name, TestData.THIRD_ROW_6_INDX),
                new Tuple2<>(TestSchema.COLUMN_THIRD_FIRST.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_SECOND.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_BOOL.name, null),
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, null));

        final Map<String, Inject> PK = TestData.pkToInject(table, ROW);

        // Execute
        final Map<String, Object> result1 = subject.insert(table.name, PK, TestData.THIRD_EXTRACT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, INJECT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        Map<String, Object> result1 = subject.insert(table.name, INJECT, TestData.THIRD_EXTRACT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        subject.insert(table.name, INJECT);

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

        final Map<String, Inject> INJECT = TestData.toInject(table, ROW);
        final Map<String, Inject> PK     = TestData.pkToInject(table, ROW);

        // Execute
        Map<String, Object> result1 = subject.insert(table.name, INJECT, TestData.THIRD_EXTRACT);

        // Verify
        Map<String, Object> result2 = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();
        assertEquals(ROW, result1);
        assertEquals(ROW, result2);
    }

}
