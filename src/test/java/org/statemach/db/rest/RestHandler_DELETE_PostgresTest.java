package org.statemach.db.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;
import org.statemach.db.sql.postgres.TestSchema;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class RestHandler_DELETE_PostgresTest extends RestHandler_Common_PostgresTest {

    static final Map<String, Object> SECOND_ROW_1_TWO = HashMap
        .ofEntries(new Tuple2<>(TestSchema.COLUMN_SECOND_TWO.name, null));

    @BeforeEach
    void restoreRows() {
        TestDB.restoreAllTablesRow2();
    }

    @Test
    void first_all() {
        runTest("delete",
                "first/" + TestData.FIRST_ROW_2_ID,
                "empty.json",
                200,
                "delete.first.all.expect.json",
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void first_id() {
        runTest("delete",
                "first/" + TestData.FIRST_ROW_2_ID + "?$select=id",
                "empty.json",
                200,
                "delete.first.id.expect.json",
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void first_varying() {
        runTest("delete",
                "first/" + TestData.FIRST_ROW_2_ID + "?$select=varying",
                "empty.json",
                200,
                "delete.first.varying.expect.json",
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void second_all() {
        // Remove FK constraint to SECOND_ROW_2
        TestDB.update(TestSchema.TABLE_INFO_SECOND, TestData.SECOND_ROW_1_PK, SECOND_ROW_1_TWO);

        runTest("delete",
                "second/${0}",
                "empty.json",
                200,
                "delete.second.all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_id() {
        // Remove FK constraint to SECOND_ROW_2
        TestDB.update(TestSchema.TABLE_INFO_SECOND, TestData.SECOND_ROW_1_PK, SECOND_ROW_1_TWO);

        runTest("delete",
                "second/${0}?$select=id",
                "empty.json",
                200,
                "delete.second.id.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void second_int() {
        // Remove FK constraint to SECOND_ROW_2
        TestDB.update(TestSchema.TABLE_INFO_SECOND, TestData.SECOND_ROW_1_PK, SECOND_ROW_1_TWO);

        runTest("delete",
                "second/${0}?$select=int",
                "empty.json",
                200,
                "delete.second.int.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void third_all() {
        runTest("delete",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX,
                "empty.json",
                200,
                "delete.third.all.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_name() {
        runTest("delete",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX + "?$select=name",
                "empty.json",
                200,
                "delete.third.name.expect.json");
    }

    @Test
    void third_bool() {
        runTest("delete",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX + "?$select=bool",
                "empty.json",
                200,
                "delete.third.bool.expect.json");
    }

}
