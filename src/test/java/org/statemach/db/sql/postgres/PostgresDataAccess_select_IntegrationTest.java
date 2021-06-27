package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.TableInfo;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.control.Option;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresDataAccess_select_IntegrationTest {

    final PostgresDataAccess subject = new PostgresDataAccess(TestDB.jdbc, TestDB.schema);

    @BeforeAll
    static void setup() {
        TestDB.setup();
        TestDB.truncateAll();
        TestDB.insertAll();
    }

    @Test
    void select_first_non_existing() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.FIRST_ROW_4_PK);

        // Execute
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT);

        // Verify
        assertFalse(result.isDefined());
    }

    @Test
    void select_first_single_column() {
        // Setup
        final TableInfo               table   = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Inject>     PK      = TestData.toInject(table, TestData.FIRST_ROW_1_PK);
        final Map<String, Extract<?>> EXTRACT = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, Extract.STRING));
        final Map<String, Object>     ROW     = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, TestData.FIRST_ROW_1_FIXED));

        // Execute
        final Map<String, Object> result = subject.select(table.name, PK, EXTRACT).get();

        // Verify
        assertEquals(ROW, result);
    }

    @Test
    void select_first_all_column() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.FIRST_ROW_1_PK);
        final Map<String, Object> ROW   = TestData.FIRST_ROW_1_PK
            .merge(TestData.FIRST_ROW_1_VAL)
            .merge(TestData.FIRST_ROW_2_REF);

        // Execute
        Map<String, Object> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT).get();

        // Verify
        assertEquals(ROW, result);
    }

    @Test
    void select_second_non_existing() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.SECOND_ROW_4_PK);

        // Execute
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT);

        // Verify
        assertFalse(result.isDefined());
    }

    @Test
    void select_second_single_column() {
        // Setup
        final TableInfo               table   = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Inject>     PK      = TestData.toInject(table, TestData.SECOND_ROW_1_PK);
        final Map<String, Extract<?>> EXTRACT = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, Extract.LONG));
        final Map<String, Object>     ROW     = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_SECOND_LONG.name, TestData.SECOND_ROW_1_LONG));

        // Execute
        final Map<String, Object> result = subject.select(table.name, PK, EXTRACT).get();

        // Verify
        assertEquals(ROW, result);
    }

    @Test
    void select_second_all_column() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_SECOND;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.SECOND_ROW_1_PK);
        final Map<String, Object> ROW   = TestData.SECOND_ROW_1_PK
            .merge(TestData.SECOND_ROW_1_VAL)
            .merge(TestData.SECOND_ROW_1_REF);

        // Execute
        Map<String, Object> result = subject.select(table.name, PK, TestData.SECOND_EXTRACT).get();

        // Verify
        assertEquals(ROW, result);
    }

    @Test
    void select_third_non_existing() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.THIRD_ROW_4_PK);

        // Execute
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT);

        // Verify
        assertFalse(result.isDefined());
    }

    @Test
    void select_third_single_column() {
        // Setup
        final TableInfo               table   = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Inject>     PK      = TestData.toInject(table, TestData.THIRD_ROW_1_PK);
        final Map<String, Extract<?>> EXTRACT = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, Extract.TIMESTAMP_AS_INSTANT));
        final Map<String, Object>     ROW     = HashMap.ofEntries(
                new Tuple2<>(TestSchema.COLUMN_THIRD_TIME.name, TestData.THIRD_ROW_1_TIME));

        // Execute
        final Map<String, Object> result = subject.select(table.name, PK, EXTRACT).get();

        // Verify
        assertEquals(ROW, result);
    }

    @Test
    void select_third_all_column() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.THIRD_ROW_1_PK);
        final Map<String, Object> ROW   = TestData.THIRD_ROW_1_PK
            .merge(TestData.THIRD_ROW_1_VAL)
            .merge(TestData.THIRD_ROW_1_REF);

        // Execute
        Map<String, Object> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT).get();

        // Verify
        assertEquals(ROW, result);
    }

}
