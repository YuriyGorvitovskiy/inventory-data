package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.util.Java;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresDataAccess_IntegrationTest {
    static final Map<String, Extract<?>> FIRST_TABLE_EXTRACT = HashMap.ofEntries(
            new Tuple2<>(PostgresTestData.COLUMN_FIRST_ID.name, Extract.LONG),
            new Tuple2<>(PostgresTestData.COLUMN_FIRST_SECOND.name, Extract.OBJECT_AS_UUID),
            new Tuple2<>(PostgresTestData.COLUMN_FIRST_THIRD_NAME.name, Extract.STRING),
            new Tuple2<>(PostgresTestData.COLUMN_FIRST_THIRD_INDX.name, Extract.INTEGER),
            new Tuple2<>(PostgresTestData.COLUMN_FIRST_FIXED.name, Extract.STRING),
            new Tuple2<>(PostgresTestData.COLUMN_FIRST_VARYING.name, Extract.STRING),
            new Tuple2<>(PostgresTestData.COLUMN_FIRST_UNLIMITED.name, Extract.STRING));

    static final String TRUNCATE = Java.resource("TruncateTable.sql");

    final PostgresDataAccess subject = new PostgresDataAccess(PostgresTestDB.jdbc, PostgresTestDB.schema);

    @BeforeAll
    static void setup() {
        PostgresTestDB.setup();
        truncate(PostgresTestData.TABLE_NAME_FIRST);
    }

    static void truncate(String table) {
        String sql = Java.format(TRUNCATE, PostgresTestDB.schema, table);

        PostgresTestDB.jdbc.execute(sql, ps -> {});
    }

    @Test
    void insert_into_first() {
        // Setup
        final Long ID_1 = 1L;
        // No Records in second table, FK constrain force null
        final UUID UUID_1 = null;
        // No Records in Third table, FK constrain force null
        final String  NAME_1 = null;
        final Integer INDX_1 = null;

        final String FIXED_1 = "Fixed 1";
        // // Fixed
        final String FIXED_1_RES = "Fixed 1                                                                                                                                                                                                                                                         ";
        final String VARYING_1   = "Varying 1";
        final String UNLIMITED_1 = "Unlimitied\n  String\r   1";

        final Map<String, Inject> ROW_1 = HashMap.ofEntries(
                new Tuple2<>(PostgresTestData.COLUMN_FIRST_ID.name, Inject.LONG.apply(ID_1)),
                new Tuple2<>(PostgresTestData.COLUMN_FIRST_SECOND.name, Inject.UUID_AS_OBJECT.apply(UUID_1)),
                new Tuple2<>(PostgresTestData.COLUMN_FIRST_THIRD_NAME.name, Inject.STRING.apply(NAME_1)),
                new Tuple2<>(PostgresTestData.COLUMN_FIRST_THIRD_INDX.name, Inject.INTEGER.apply(INDX_1)),
                new Tuple2<>(PostgresTestData.COLUMN_FIRST_FIXED.name, Inject.STRING.apply(FIXED_1)),
                new Tuple2<>(PostgresTestData.COLUMN_FIRST_VARYING.name, Inject.STRING.apply(VARYING_1)),
                new Tuple2<>(PostgresTestData.COLUMN_FIRST_UNLIMITED.name, Inject.STRING.apply(UNLIMITED_1)));

        // Execute
        Map<String, Object> result = subject.insert(PostgresTestData.TABLE_NAME_FIRST, ROW_1, FIRST_TABLE_EXTRACT);

        // Verify
        assertEquals(
                HashMap.ofEntries(
                        new Tuple2<>(PostgresTestData.COLUMN_FIRST_ID.name, ID_1),
                        new Tuple2<>(PostgresTestData.COLUMN_FIRST_SECOND.name, UUID_1),
                        new Tuple2<>(PostgresTestData.COLUMN_FIRST_THIRD_NAME.name, NAME_1),
                        new Tuple2<>(PostgresTestData.COLUMN_FIRST_THIRD_INDX.name, INDX_1),
                        new Tuple2<>(PostgresTestData.COLUMN_FIRST_FIXED.name, FIXED_1_RES),
                        new Tuple2<>(PostgresTestData.COLUMN_FIRST_VARYING.name, VARYING_1),
                        new Tuple2<>(PostgresTestData.COLUMN_FIRST_UNLIMITED.name, UNLIMITED_1)),
                result);

    }
}
