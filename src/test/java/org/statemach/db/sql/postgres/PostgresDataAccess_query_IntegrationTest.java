package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.jdbc.Injector;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.Join.Kind;
import org.statemach.db.sql.Select;
import org.statemach.db.sql.TableLike;
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

    static final String CTE_NAME_1 = "cte1";
    static final String CTE_NAME_2 = "cte2";

    static final String CTE_COLUMN_ID     = "id";
    static final String CTE_COLUMN_DOUBLE = "dbl";

    static final TableLike TABLE_FIRST   = TableLike.of(TestSchema.SCHEMA, TestSchema.TABLE_INFO_FIRST);
    static final TableLike TABLE_SECOND  = TableLike.of(TestSchema.SCHEMA, TestSchema.TABLE_INFO_SECOND);
    static final TableLike TABLE_THIRD   = TableLike.of(TestSchema.SCHEMA, TestSchema.TABLE_INFO_THIRD);
    static final TableLike TABLE_VERSION = TableLike.of(TestSchema.SCHEMA, TestSchema.TABLE_INFO_VERSION);

    static final TableLike CTE_1 = TableLike.of(CTE_NAME_1, Inject.NOTHING);
    static final TableLike CTE_2 = TableLike.of(CTE_NAME_2, Inject.NOTHING);

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
    void query_first_text_search() {
        // Setup

        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                NodeLinkTree.<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1)),
                subject.builder().textSearch(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_SEARCH.name),
                        List.of("fix", "vary", "2")),
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE)),
                List.of(
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, Extract.LONG))),
                true,
                null,
                null);

        // Execute
        final List<Map<String, Object>> result = subject.query(List.empty(), query);

        // Verify
        assertEquals(
                List.of(TestData.FIRST_ROW_2_PK),
                result);
    }

    @Test
    void query_first_not_in() {
        // Setup
        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                NodeLinkTree.<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1)),
                subject.builder().not(
                        subject.builder().in(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name),
                                List.of(Injector.LONG.prepare(TestData.FIRST_ROW_2_ID),
                                        Injector.LONG.prepare(TestData.FIRST_ROW_3_ID),
                                        Injector.LONG.prepare(-1L)))),
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE)),
                List.of(
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, Extract.LONG))),
                true,
                null,
                null);

        // Execute
        final List<Map<String, Object>> result = subject.query(List.empty(), query);

        // Verify
        assertEquals(
                List.of(TestData.FIRST_ROW_1_PK),
                result);
    }

    @Test
    void query_first_in_array() {
        // Setup
        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                NodeLinkTree.<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1)),
                subject.builder().inArray(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, null),
                        PostgresDataType.BIGINT,
                        List.of(TestData.FIRST_ROW_2_ID, TestData.FIRST_ROW_3_ID, -1)),
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE)),
                List.of(
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, Extract.LONG))),
                true,
                null,
                null);

        // Execute
        final List<Map<String, Object>> result = subject.query(List.empty(), query);

        // Verify
        assertEquals(
                List.of(TestData.FIRST_ROW_2_PK, TestData.FIRST_ROW_3_PK),
                result);
    }

    @Test
    void query_first_in_null_array() {
        // Setup
        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                NodeLinkTree.<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1)),
                subject.builder().inArray(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, null),
                        PostgresDataType.BIGINT,
                        null),
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE)),
                List.of(
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, Extract.LONG))),
                true,
                null,
                null);

        // Execute
        final List<Map<String, Object>> result = subject.query(List.empty(), query);

        // Verify
        assertEquals(List.empty(), result);
    }

    @Test
    void query_first_all_columns() {
        // Setup

        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                NodeLinkTree.<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1)),
                Condition.NONE,
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE)),
                List.of(
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_ID.name, Extract.LONG)),
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_SECOND.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_SECOND.name, Extract.OBJECT_AS_UUID)),
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_THIRD_NAME.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_NAME.name, Extract.STRING)),
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_THIRD_INDX.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_THIRD_INDX.name, Extract.INTEGER)),
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_FIXED.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_FIXED.name, Extract.STRING)),
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_VARYING.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_VARYING.name, Extract.STRING)),
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_UNLIMITED.name,
                                new Tuple2<>(TestSchema.COLUMN_FIRST_UNLIMITED.name, Extract.STRING))),
                true,
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
            .<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1))
            .put(TestSchema.FK_FIRST_SECOND.name,
                    new Join(Kind.LEFT,
                            subject.builder().equal(
                                    Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_SECOND.name),
                                    Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name))),
                    NodeLinkTree.<String, From, Join>of(new From(TABLE_SECOND, ALIAS_2)))
            .put(TestSchema.FK_FIRST_THIRD.name,
                    new Join(Kind.RIGHT,
                            subject.builder().and(
                                    subject.builder().equal(
                                            Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_THIRD_NAME.name),
                                            Select.of(ALIAS_3, TestSchema.COLUMN_THIRD_NAME.name)),
                                    subject.builder().equal(
                                            Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_THIRD_INDX.name),
                                            Select.of(ALIAS_3, TestSchema.COLUMN_THIRD_INDX.name)))),
                    NodeLinkTree.<String, From, Join>of(new From(TABLE_THIRD, ALIAS_3)));

        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                joins,
                Condition.NONE,
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE),
                        Select.of(ALIAS_3, TestSchema.COLUMN_THIRD_NAME.name, Boolean.FALSE)),
                List.of(
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(EXTRACT_NAME_1, Extract.LONG)),
                        Select.of(ALIAS_2,
                                TestSchema.COLUMN_SECOND_ID.name,
                                new Tuple2<>(EXTRACT_NAME_2, Extract.OBJECT_AS_UUID)),
                        Select.of(ALIAS_3,
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
            .<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1))
            .put(TestSchema.FK_SECOND_FIRST.name,
                    new Join(Kind.INNER,
                            subject.builder().equal(
                                    Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name),
                                    Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_FIRST.name))),
                    NodeLinkTree.<String, From, Join>of(new From(TABLE_SECOND, ALIAS_2))
                        .put(TestSchema.FK_THIRD_SECOND.name,
                                new Join(Kind.FULL,
                                        subject.builder().equal(
                                                Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name),
                                                Select.of(ALIAS_3, TestSchema.COLUMN_THIRD_SECOND.name))),
                                NodeLinkTree.<String, From, Join>of(new From(TABLE_THIRD, ALIAS_3))));

        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                joins,
                Condition.NONE,
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, Boolean.TRUE),
                        Select.of(ALIAS_3, TestSchema.COLUMN_THIRD_NAME.name, Boolean.FALSE)),
                List.of(
                        Select.of(ALIAS_1,
                                TestSchema.COLUMN_FIRST_ID.name,
                                new Tuple2<>(EXTRACT_NAME_1, Extract.LONG)),
                        Select.of(ALIAS_2,
                                TestSchema.COLUMN_SECOND_ID.name,
                                new Tuple2<>(EXTRACT_NAME_2, Extract.OBJECT_AS_UUID)),
                        Select.of(ALIAS_3,
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

    @Test
    void query_cte1_inner_cte2() {
        // Setup
        // Common Table Expression 1
        NodeLinkTree<String, From, Join> joins1 = NodeLinkTree
            .<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1))
            .put(TestSchema.FK_SECOND_FIRST.name,
                    new Join(Kind.LEFT,
                            subject.builder().equal(
                                    Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name),
                                    Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_FIRST.name))),
                    NodeLinkTree.<String, From, Join>of(new From(TABLE_SECOND, ALIAS_2))
                        .put(TestSchema.FK_SECOND_SECOND_ONE.name,
                                new Join(Kind.LEFT,
                                        subject.builder().or(
                                                subject.builder().equal(
                                                        Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name),
                                                        Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_ONE.name)),
                                                subject.builder().equal(
                                                        Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name),
                                                        Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_TWO.name)))),
                                NodeLinkTree.<String, From, Join>of(new From(TABLE_SECOND, ALIAS_3))));

        View<String> cte1 = new View<String>(CTE_NAME_1,
                joins1,
                subject.builder().or(
                        subject.builder().equal(
                                Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_DOUBLE.name),
                                Injector.DOUBLE.prepare(TestData.SECOND_ROW_1_DOUBLE)),
                        subject.builder().isNull(
                                Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_DOUBLE.name))),
                List.empty(),
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, CTE_COLUMN_ID),
                        Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_DOUBLE.name, CTE_COLUMN_DOUBLE)),
                false,
                null,
                null);

        // Common Table Expression 2
        NodeLinkTree<String, From, Join> joins2 = NodeLinkTree
            .<String, From, Join>of(new From(TABLE_FIRST, ALIAS_1))
            .put(TestSchema.FK_FIRST_SECOND.name,
                    new Join(Kind.LEFT,
                            subject.builder().equal(
                                    Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_SECOND.name),
                                    Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name))),
                    NodeLinkTree.<String, From, Join>of(new From(TABLE_SECOND, ALIAS_2))
                        .put(TestSchema.FK_SECOND_SECOND_ONE.name,
                                new Join(Kind.LEFT,
                                        subject.builder().or(
                                                subject.builder().equal(
                                                        Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name),
                                                        Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_ONE.name)),
                                                subject.builder().equal(
                                                        Select.of(ALIAS_2, TestSchema.COLUMN_SECOND_ID.name),
                                                        Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_TWO.name)))),
                                NodeLinkTree.<String, From, Join>of(new From(TABLE_SECOND, ALIAS_3))));

        View<String> cte2 = new View<String>(CTE_NAME_2,
                joins2,
                subject.builder().isNotNull(
                        Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_DOUBLE.name)),
                List.empty(),
                List.of(Select.of(ALIAS_1, TestSchema.COLUMN_FIRST_ID.name, CTE_COLUMN_ID),
                        Select.of(ALIAS_3, TestSchema.COLUMN_SECOND_DOUBLE.name, CTE_COLUMN_DOUBLE)),
                false,
                null,
                null);

        // Query
        NodeLinkTree<String, From, Join> joins = NodeLinkTree
            .<String, From, Join>of(new From(CTE_1, ALIAS_1))
            .put("ByID",
                    new Join(Kind.INNER,
                            subject.builder().equal(
                                    Select.of(ALIAS_1, CTE_COLUMN_ID),
                                    Select.of(ALIAS_2, CTE_COLUMN_ID))),
                    NodeLinkTree.<String, From, Join>of(new From(CTE_2, ALIAS_2)));

        View<Tuple2<String, Extract<?>>> query = new View<Tuple2<String, Extract<?>>>("",
                joins,
                Condition.NONE,
                List.of(Select.of(ALIAS_1, CTE_COLUMN_ID, Boolean.TRUE),
                        Select.of(ALIAS_2, CTE_COLUMN_DOUBLE, Boolean.FALSE)),
                List.of(
                        Select.of(ALIAS_1, CTE_COLUMN_ID, new Tuple2<>(EXTRACT_NAME_1, Extract.LONG)),
                        Select.of(ALIAS_1, CTE_COLUMN_DOUBLE, new Tuple2<>(EXTRACT_NAME_2, Extract.DOUBLE)),
                        Select.of(ALIAS_2, CTE_COLUMN_DOUBLE, new Tuple2<>(EXTRACT_NAME_3, Extract.DOUBLE))),
                false,
                1L,
                1);

        // Execute
        final List<Map<String, Object>> result = subject.query(List.of(cte1, cte2), query);

        // Verify
        assertEquals(List.of(
                HashMap.ofEntries(
                        new Tuple2<>(EXTRACT_NAME_1, TestData.FIRST_ROW_1_ID),
                        new Tuple2<>(EXTRACT_NAME_2, TestData.SECOND_ROW_1_DOUBLE),
                        new Tuple2<>(EXTRACT_NAME_3, TestData.SECOND_ROW_1_DOUBLE))),
                result);
    }

    @Test
    void arrayAsTable_singleColumn() {
        // Setup
        ColumnInfo   info = ColumnInfo.of("id", PostgresDataType.UUID);
        List<Object> data = List.of(TestData.SECOND_ROW_1_ID.toString(), TestData.SECOND_ROW_2_ID.toString(), null);

        TableLike from = subject.builder.arrayAsTable(info, data);
        String    sql  = "SELECT a.id FROM " + from.sql + " a";

        // Execute
        List<UUID> result = subject.jdbc.query(sql,
                ps -> from.inject.set(ps, 1),
                rs -> Extract.OBJECT_AS_UUID.get(rs, 1));

        // Verify
        assertEquals(
                List.of(TestData.SECOND_ROW_1_ID, TestData.SECOND_ROW_2_ID, null),
                result);
    }

    @Test
    void arrayAsTable_customType() {
        // Setup
        List<Map<String, Object>> data = List.of(
                HashMap.ofEntries(
                        new Tuple2<>(TestSchema.COLUMN_CUSTOM_STR.name, "Hello"),
                        new Tuple2<>(TestSchema.COLUMN_CUSTOM_NUM.name, 123L),
                        new Tuple2<>(TestSchema.COLUMN_CUSTOM_TIME.name, TestData.THIRD_ROW_1_TIME)),
                HashMap.ofEntries(
                        new Tuple2<>(TestSchema.COLUMN_CUSTOM_STR.name, "World"),
                        new Tuple2<>(TestSchema.COLUMN_CUSTOM_NUM.name, "345"),
                        new Tuple2<>(TestSchema.COLUMN_CUSTOM_TIME.name, TestData.THIRD_ROW_2_TIME)),
                HashMap.ofEntries(
                        new Tuple2<>(TestSchema.COLUMN_CUSTOM_NUM.name, 567),
                        new Tuple2<>(TestSchema.COLUMN_CUSTOM_TIME.name, null)));

        TableLike from = subject.builder.arrayAsTable(TestSchema.TYPE_CUSTOM, TestSchema.CUSTOM_COLUMNS, data);
        String    sql  = "SELECT a.str, a.num, a.time FROM " + from.sql + " a ORDER BY a.num ASC";

        // Execute
        List<List<Object>> result = subject.jdbc.query(sql,
                ps -> from.inject.set(ps, 1),
                rs -> List.of(
                        Extract.STRING.get(rs, 1),
                        Extract.LONG.get(rs, 2),
                        Extract.TIMESTAMP_AS_INSTANT.get(rs, 3)));

        // Verify
        assertEquals(
                List.of(
                        List.of("Hello", 123L, TestData.THIRD_ROW_1_TIME),
                        List.of("World", 345L, TestData.THIRD_ROW_2_TIME),
                        List.of(null, 567L, null)),
                result);

    }
}
