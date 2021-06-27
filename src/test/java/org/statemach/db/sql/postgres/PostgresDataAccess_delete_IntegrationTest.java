package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.TableInfo;

import io.vavr.collection.Map;
import io.vavr.control.Option;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresDataAccess_delete_IntegrationTest {

    final PostgresDataAccess subject = new PostgresDataAccess(TestDB.jdbc, TestDB.schema);

    @BeforeAll
    static void setup() {
        TestDB.setup();
        TestDB.truncateAll();
        TestDB.insertAll();
    }

    @AfterEach
    void restore() {
        TestDB.restoreAllTablesRow2();
    }

    @Test
    void delete_first_non_existing() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.FIRST_ROW_4_PK);

        // Execute
        final boolean success = subject.delete(table.name, PK);

        // Verify
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT);
        assertFalse(success);
        assertFalse(result.isDefined());
    }

    @Test
    void delete_first_non_existing_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.FIRST_ROW_4_PK);

        // Execute
        final Option<Map<String, Object>> result = subject.delete(table.name, PK, TestData.FIRST_EXTRACT);

        // Verify
        final Option<Map<String, Object>> result1 = subject.select(table.name, PK, TestData.FIRST_EXTRACT);
        assertFalse(result.isDefined());
        assertFalse(result1.isDefined());
    }

    @Test
    void delete_first() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.FIRST_ROW_2_PK);

        // Execute
        final boolean success = subject.delete(table.name, PK);

        // Verify
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.FIRST_EXTRACT);
        assertTrue(success);
        assertFalse(result.isDefined());
    }

    @Test
    void delete_first_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_FIRST;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.FIRST_ROW_2_PK);
        final Map<String, Object> ROW   = TestData.FIRST_ROW_2_PK
            .merge(TestData.FIRST_ROW_2_VAL)
            .merge(TestData.FIRST_ROW_2_REF);

        // Execute
        final Map<String, Object> result = subject.delete(table.name, PK, TestData.FIRST_EXTRACT).get();

        // Verify
        final Option<Map<String, Object>> result1 = subject.select(table.name, PK, TestData.FIRST_EXTRACT);
        assertEquals(ROW, result);
        assertFalse(result1.isDefined());
    }

    @Test
    void delete_thrid_non_existing() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.THIRD_ROW_4_PK);

        // Execute
        final boolean success = subject.delete(table.name, PK);

        // Verify
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT);
        assertFalse(success);
        assertFalse(result.isDefined());
    }

    @Test
    void delete_thrid_non_existing_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.THIRD_ROW_4_PK);

        // Execute
        final Option<Map<String, Object>> result = subject.delete(table.name, PK, TestData.THIRD_EXTRACT);

        // Verify
        final Option<Map<String, Object>> result1 = subject.select(table.name, PK, TestData.THIRD_EXTRACT);
        assertFalse(result.isDefined());
        assertFalse(result1.isDefined());
    }

    @Test
    void delete_thrid() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.THIRD_ROW_2_PK);

        // Execute
        final boolean success = subject.delete(table.name, PK);

        // Verify
        final Option<Map<String, Object>> result = subject.select(table.name, PK, TestData.THIRD_EXTRACT);
        assertTrue(success);
        assertFalse(result.isDefined());
    }

    @Test
    void delete_thrid_returning() {
        // Setup
        final TableInfo           table = TestSchema.TABLE_INFO_THIRD;
        final Map<String, Inject> PK    = TestData.toInject(table, TestData.THIRD_ROW_2_PK);
        final Map<String, Object> ROW   = TestData.THIRD_ROW_2_PK
            .merge(TestData.THIRD_ROW_2_VAL)
            .merge(TestData.THIRD_ROW_2_REF);

        // Execute
        final Map<String, Object> result = subject.delete(table.name, PK, TestData.THIRD_EXTRACT).get();

        // Verify
        final Option<Map<String, Object>> result1 = subject.select(table.name, PK, TestData.THIRD_EXTRACT);
        assertEquals(ROW, result);
        assertFalse(result1.isDefined());
    }
}
