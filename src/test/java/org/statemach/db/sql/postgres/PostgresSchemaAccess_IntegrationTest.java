package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.PrimaryKey;
import org.statemach.db.schema.Schema;

import io.vavr.collection.List;
import io.vavr.collection.Map;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresSchemaAccess_IntegrationTest {

    final PostgresSchemaAccess subject = new PostgresSchemaAccess(TestDB.jdbc, TestDB.schema);

    @BeforeAll
    static void setup() {
        TestDB.setup();
    }

    @Test
    void getSchemaName() {
        // Execute
        String result = subject.getSchemaName();

        // Verify
        assertEquals(TestDB.schema, result);
    }

    @Test
    void getAllTables() {
        // Execute
        Map<String, List<ColumnInfo>> result = subject.getAllTables();

        // Verify
        assertEquals(TestSchema.ALL_TABLES, result);
    }

    @Test
    void getAllPrimaryKeys() {
        // Execute
        List<PrimaryKey> result = subject.getAllPrimaryKeys();

        // Verify
        assertEquals(TestSchema.ALL_PRIMARY_KEYS, result.sortBy(p -> p.name));
    }

    @Test
    void getAllForeignKeys() {
        // Execute
        List<ForeignKey> result = subject.getAllForeignKeys();

        // Verify
        assertEquals(TestSchema.ALL_FOREIGN_KEYS, result.sortBy(f -> f.name));
    }

    @Test
    void schema_from() {
        // Execute
        Schema result = Schema.from(subject);

        // Verify
        assertEquals(new Schema(TestDB.schema, TestSchema.ALL_TABLE_INFO_MAP), result);
    }

}
