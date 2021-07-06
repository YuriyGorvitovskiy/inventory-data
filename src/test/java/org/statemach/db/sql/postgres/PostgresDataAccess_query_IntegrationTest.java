package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Extract;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.Select;
import org.statemach.db.sql.View;
import org.statemach.util.NodeLinkTree;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresDataAccess_query_IntegrationTest {

    static final String ALIAS = "a";

    final PostgresDataAccess subject = new PostgresDataAccess(
            TestDB.jdbc,
            TestDB.schema,
            new PostgresSQLBuilder(TestDB.schema));

    @BeforeAll
    static void setup() {
        TestDB.setup();
        TestDB.truncateAll();
        TestDB.insertAll();
    }

    @Test
    void query_first_all_columns() {
        // Setup

        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                NodeLinkTree.<String, From, Join>of(new From(TestSchema.TABLE_NAME_FIRST, ALIAS)),
                Condition.NONE,
                List.of(new Select<>(ALIAS, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE)),
                List.of(
                        new Select<>(ALIAS,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, Extract.LONG)),
                        new Select<>(ALIAS,
                                TestSchema.COLUMN_FIRST_SECOND.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, Extract.OBJECT_AS_UUID)),
                        new Select<>(ALIAS,
                                TestSchema.COLUMN_FIRST_THIRD_NAME.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, Extract.STRING)),
                        new Select<>(ALIAS,
                                TestSchema.COLUMN_FIRST_THIRD_INDX.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, Extract.INTEGER)),
                        new Select<>(ALIAS,
                                TestSchema.COLUMN_FIRST_FIXED.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, Extract.STRING)),
                        new Select<>(ALIAS,
                                TestSchema.COLUMN_FIRST_VARYING.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, Extract.STRING)),
                        new Select<>(ALIAS,
                                TestSchema.COLUMN_FIRST_UNLIMITED.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, Extract.STRING))),
                false,
                null,
                null);

        // Execute
        final List<Map<String, Object>> result = subject.query(List.empty(), query);

        // Verify
        assertEquals(
                List.of(
                        TestData.FIRST_ROW_1_PK
                            .merge(TestData.FIRST_ROW_1_REF)
                            .merge(TestData.FIRST_ROW_1_VAL),
                        TestData.FIRST_ROW_2_PK
                            .merge(TestData.FIRST_ROW_2_REF)
                            .merge(TestData.FIRST_ROW_2_VAL),
                        TestData.FIRST_ROW_3_PK
                            .merge(TestData.FIRST_ROW_3_REF)
                            .merge(TestData.FIRST_ROW_3_VAL)),
                result);
    }
}
