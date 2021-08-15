package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.CompositeType;

import io.vavr.collection.List;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresSchemaAccess_CompositeType_IntegrationTest {

    final ColumnInfo    FIELD_1 = ColumnInfo.of("f_string", PostgresDataType.CHARACTER_VARYING, 256);
    final ColumnInfo    FIELD_2 = ColumnInfo.of("f_integer", PostgresDataType.INTEGER);
    final ColumnInfo    FIELD_3 = ColumnInfo.of("f_long", PostgresDataType.INTEGER);
    final ColumnInfo    FIELD_4 = ColumnInfo.of("f_boolean", PostgresDataType.BOOLEAN);
    final CompositeType TYPE_1  = new CompositeType("integration_test_1", List.of(FIELD_1, FIELD_2));
    final CompositeType TYPE_2  = new CompositeType("integration_test_2", List.of(FIELD_3, FIELD_4));

    final PostgresSchemaAccess subject = new PostgresSchemaAccess(TestDB.jdbc, TestDB.schema);

    @BeforeAll
    static void setup() {
        TestDB.setup();
    }

    @BeforeEach
    void create() {
        subject.createCompositeType(TYPE_1);
    }

    @AfterEach
    void drop() {
        List<CompositeType> types = subject.getAllCompositeTypes();
        if (types.contains(TYPE_1)) {
            subject.dropCompositeType(TYPE_1.name);
        }
        if (types.contains(TYPE_2)) {
            subject.dropCompositeType(TYPE_2.name);
        }
    }

    @Test
    void getAllCompositeTypes() {
        // Execute
        final List<CompositeType> result = subject.getAllCompositeTypes();

        // Verify
        assertTrue(result.contains(TYPE_1));
    }

    @Test
    void createCompositeType() {
        // Execute
        subject.createCompositeType(TYPE_2);

        // Verify
        List<CompositeType> types = subject.getAllCompositeTypes();
        assertTrue(types.contains(TYPE_2));
    }

    @Test
    void dropCompositeType() {
        // Execute
        subject.dropCompositeType(TYPE_1.name);

        // Verify
        List<CompositeType> types = subject.getAllCompositeTypes();
        assertFalse(types.contains(TYPE_1));
    }
}