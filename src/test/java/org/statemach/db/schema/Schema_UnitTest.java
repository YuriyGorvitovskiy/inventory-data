package org.statemach.db.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.statemach.db.sql.SchemaAccess;
import org.statemach.db.sql.postgres.PostgresTestData;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class Schema_UnitTest {

    final String SCHEMA_NAME_1 = "Schema1";
    final String SCHEMA_NAME_2 = "Schema2";

    final String TABLE_NAME_1 = "Table1";
    final String TABLE_NAME_2 = "Table2";
    final String TABLE_NAME_3 = "Table3";

    final TableInfo TABLE_1 = new TableInfo(TABLE_NAME_1, HashMap.empty(), null, HashMap.empty(), HashMap.empty());
    final TableInfo TABLE_2 = new TableInfo(TABLE_NAME_2, HashMap.empty(), null, HashMap.empty(), HashMap.empty());
    final TableInfo TABLE_3 = new TableInfo(TABLE_NAME_3, HashMap.empty(), null, HashMap.empty(), HashMap.empty());

    final Map<String, TableInfo> TABLES_1 = List.of(TABLE_1, TABLE_2).toLinkedMap(t -> t.name, t -> t);
    final Map<String, TableInfo> TABLES_2 = List.of(TABLE_2, TABLE_1).toLinkedMap(t -> t.name, t -> t);
    final Map<String, TableInfo> TABLES_3 = List.of(TABLE_1, TABLE_3).toLinkedMap(t -> t.name, t -> t);
    final Map<String, TableInfo> TABLES_4 = List.of(TABLE_1).toLinkedMap(t -> t.name, t -> t);

    final Schema subject = new Schema(SCHEMA_NAME_1, TABLES_1);
    final Schema other1  = new Schema(SCHEMA_NAME_1, TABLES_1);
    final Schema other2  = new Schema(SCHEMA_NAME_2, TABLES_1);
    final Schema other3  = new Schema(SCHEMA_NAME_1, TABLES_2);
    final Schema other4  = new Schema(SCHEMA_NAME_1, TABLES_3);
    final Schema other5  = new Schema(SCHEMA_NAME_1, TABLES_4);

    @Test
    void from_postgres() {
        // Setup
        SchemaAccess access = mock(SchemaAccess.class);
        doReturn(SCHEMA_NAME_1).when(access).getSchemaName();
        doReturn(PostgresTestData.ALL_TABLES).when(access).getAllTables();
        doReturn(PostgresTestData.ALL_PRIMARY_KEYS).when(access).getAllPrimaryKeys();
        doReturn(PostgresTestData.ALL_FOREIGN_KEYS).when(access).getAllForeignKeys();

        // Execute
        Schema result = Schema.from(access);

        // Verify
        assertEquals(new Schema(SCHEMA_NAME_1, PostgresTestData.ALL_TABLE_INFO_MAP), result);
    }

    @Test
    void hashCode_test() {
        // Execute
        int result  = subject.hashCode();
        int result1 = other1.hashCode();
        int result2 = other3.hashCode();

        // Verify
        assertEquals(result, result1);
        assertEquals(result, result2);
    }

    @Test
    void equals_test() {
        // Execute
        boolean result  = subject.equals(subject);
        boolean result1 = subject.equals(other1);
        boolean result2 = subject.equals(other2);
        boolean result3 = subject.equals(other3);
        boolean result4 = subject.equals(other4);
        boolean result5 = subject.equals(other5);

        // Verify
        assertTrue(result);
        assertTrue(result1);
        assertFalse(result2);
        assertTrue(result3);
        assertFalse(result4);
        assertFalse(result5);
    }
}
