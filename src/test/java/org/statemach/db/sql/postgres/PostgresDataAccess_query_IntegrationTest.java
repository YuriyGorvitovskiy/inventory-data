package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Extract;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.Join.Kind;
import org.statemach.db.sql.Select;
import org.statemach.db.sql.View;
import org.statemach.util.NodeLinkTree;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class PostgresDataAccess_query_IntegrationTest {

    static final String ALIAS_1 = "a1";
    static final String ALIAS_2 = "a2";
    static final String ALIAS_3 = "a3";

    static final String EXTRACT_NAME_1 = "column.1";
    static final String EXTRACT_NAME_2 = "column.2";
    static final String EXTRACT_NAME_3 = "column.3";

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
                NodeLinkTree.<String, From, Join>of(new From(TestSchema.TABLE_NAME_FIRST, ALIAS_1)),
                Condition.NONE,
                List.of(new Select<>(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE)),
                List.of(
                        new Select<>(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, Extract.LONG)),
                        new Select<>(ALIAS_1,
                                TestSchema.COLUMN_FIRST_SECOND.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, Extract.OBJECT_AS_UUID)),
                        new Select<>(ALIAS_1,
                                TestSchema.COLUMN_FIRST_THIRD_NAME.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, Extract.STRING)),
                        new Select<>(ALIAS_1,
                                TestSchema.COLUMN_FIRST_THIRD_INDX.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, Extract.INTEGER)),
                        new Select<>(ALIAS_1,
                                TestSchema.COLUMN_FIRST_FIXED.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, Extract.STRING)),
                        new Select<>(ALIAS_1,
                                TestSchema.COLUMN_FIRST_VARYING.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, Extract.STRING)),
                        new Select<>(ALIAS_1,
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

    @Test
    void query_first_left_second_and_right_third() {
        // Setup
        NodeLinkTree<String, From, Join> joins = NodeLinkTree
            .<String, From, Join>of(new From(TestSchema.TABLE_NAME_FIRST, ALIAS_1))
            .put(TestSchema.FK_FIRST_SECOND.name,
                    new Join(Kind.LEFT,
                            subject.builder.equal(
                                    new Select<Void>(ALIAS_1, TestSchema.COLUMN_FIRST_SECOND.name, null),
                                    new Select<Void>(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name, null))),
                    NodeLinkTree.<String, From, Join>of(new From(TestSchema.TABLE_NAME_SECOND, ALIAS_2)))
            .put(TestSchema.FK_FIRST_THIRD.name,
                    new Join(Kind.RIGHT,
                            subject.builder.and(
                                    subject.builder.equal(
                                            new Select<Void>(ALIAS_1, TestSchema.COLUMN_FIRST_THIRD_NAME.name, null),
                                            new Select<Void>(ALIAS_3, TestSchema.COLUMN_THIRD_NAME.name, null)),
                                    subject.builder.equal(
                                            new Select<Void>(ALIAS_1, TestSchema.COLUMN_FIRST_THIRD_INDX.name, null),
                                            new Select<Void>(ALIAS_3, TestSchema.COLUMN_THIRD_INDX.name, null)))),
                    NodeLinkTree.<String, From, Join>of(new From(TestSchema.TABLE_NAME_THIRD, ALIAS_3)));

        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                joins,
                Condition.NONE,
                List.of(new Select<>(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE),
                        new Select<>(ALIAS_3, TestSchema.COLUMN_THIRD_NAME.name, Boolean.FALSE)),
                List.of(
                        new Select<>(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(EXTRACT_NAME_1, Extract.LONG)),
                        new Select<>(ALIAS_2,
                                TestSchema.COLUMN_SECOND_ID.name,
                                new Tuple2<>(EXTRACT_NAME_2, Extract.OBJECT_AS_UUID)),
                        new Select<>(ALIAS_3,
                                TestSchema.COLUMN_THIRD_NAME.name,
                                new Tuple2<>(EXTRACT_NAME_3, Extract.STRING))),
                false,
                null,
                null);

        // Execute
        final List<Map<String, Object>> result = subject.query(List.empty(), query);

        // Verify
        assertEquals(List.of(
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, TestData.FIRST_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_2, TestData.SECOND_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_3, TestData.THIRD_ROW_1_NAME)),
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, TestData.FIRST_ROW_2_ID),
                        new Tuple2<>(EXTRACT_NAME_2, TestData.SECOND_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_3, TestData.THIRD_ROW_1_NAME)),
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, null),
                        new Tuple2<>(EXTRACT_NAME_2, null),
                        new Tuple2<>(EXTRACT_NAME_3, TestData.THIRD_ROW_3_NAME)),
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, null),
                        new Tuple2<>(EXTRACT_NAME_2, null),
                        new Tuple2<>(EXTRACT_NAME_3, TestData.THIRD_ROW_2_NAME))),
                result);
    }

    @Test
    void query_first_inner_second_chain_full_third() {
        // Setup
        NodeLinkTree<String, From, Join> joins = NodeLinkTree
            .<String, From, Join>of(new From(TestSchema.TABLE_NAME_FIRST, ALIAS_1))
            .put(TestSchema.FK_SECOND_FIRST.name,
                    new Join(Kind.INNER,
                            subject.builder.equal(
                                    new Select<Void>(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, null),
                                    new Select<Void>(ALIAS_2, TestSchema.COLUMN_SECOND_FIRST.name, null))),
                    NodeLinkTree.<String, From, Join>of(new From(TestSchema.TABLE_NAME_SECOND, ALIAS_2))
                        .put(TestSchema.FK_THIRD_SECOND.name,
                                new Join(Kind.FULL,
                                        subject.builder.equal(
                                                new Select<Void>(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name, null),
                                                new Select<Void>(ALIAS_3, TestSchema.COLUMN_THIRD_SECOND.name, null))),
                                NodeLinkTree.<String, From, Join>of(new From(TestSchema.TABLE_NAME_THIRD, ALIAS_3))));

        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                joins,
                Condition.NONE,
                List.of(new Select<>(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE),
                        new Select<>(ALIAS_3, TestSchema.COLUMN_THIRD_NAME.name, Boolean.FALSE)),
                List.of(
                        new Select<>(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(EXTRACT_NAME_1, Extract.LONG)),
                        new Select<>(ALIAS_2,
                                TestSchema.COLUMN_SECOND_ID.name,
                                new Tuple2<>(EXTRACT_NAME_2, Extract.OBJECT_AS_UUID)),
                        new Select<>(ALIAS_3,
                                TestSchema.COLUMN_THIRD_NAME.name,
                                new Tuple2<>(EXTRACT_NAME_3, Extract.STRING))),
                false,
                null,
                null);

        // Execute
        final List<Map<String, Object>> result = subject.query(List.empty(), query);

        // Verify
        assertEquals(List.of(
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, TestData.FIRST_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_2, TestData.SECOND_ROW_2_ID),
                        new Tuple2<>(EXTRACT_NAME_3, null)),
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, TestData.FIRST_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_2, TestData.SECOND_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_3, TestData.THIRD_ROW_2_NAME)),
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, TestData.FIRST_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_2, TestData.SECOND_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_3, TestData.THIRD_ROW_1_NAME)),
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, null),
                        new Tuple2<>(EXTRACT_NAME_2, null),
                        new Tuple2<>(EXTRACT_NAME_3, TestData.THIRD_ROW_3_NAME))),
                result);
    }
}
