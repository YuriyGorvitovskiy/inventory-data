package org.statemach.db.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class RestHandler_PUT_PostgresTest extends RestHandler_Common_PostgresTest {

    @BeforeEach
    void restoreRows() {
        TestDB.restoreAllTablesRow2();
    }

    @Test
    void insert_first_second_all() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_4_ID,
                "put.insert.first.second-all.json",
                200,
                "put.insert.first.second-all.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void insert_first_null_all() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_5_ID,
                "put.insert.first.null-all.json",
                200,
                "put.insert.first.null-all.expect.json");
    }

    @Test
    void insert_first_all() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_6_ID,
                "put.insert.first.all.json",
                200,
                "put.insert.first.all.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void insert_first_id() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_7_ID + "?$select=id",
                "put.insert.first.id.json",
                200,
                "put.insert.first.id.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void insert_first_varying() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_8_ID + "?$select=varying",
                "put.insert.first.varying.json",
                200,
                "put.insert.first.varying.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void insert_second_pk_only_all() {
        runTest("put",
                "second/${0}",
                "put.insert.second.short-all.json",
                200,
                "put.insert.second.short-all.expect.json",
                TestData.SECOND_ROW_4_ID);
    }

    @Test
    void insert_second_null_all() {
        runTest("put",
                "second/${0}",
                "put.insert.second.null-all.json",
                200,
                "put.insert.second.null-all.expect.json",
                TestData.SECOND_ROW_5_ID);
    }

    @Test
    void insert_second_all() {
        runTest("put",
                "second/${0}",
                "put.insert.second.all.json",
                200,
                "put.insert.second.all.expect.json",
                TestData.SECOND_ROW_6_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_6_ID);
    }

    @Test
    void insert_second_id() {
        runTest("put",
                "second/${0}?$select=id",
                "put.insert.second.id.json",
                200,
                "put.insert.second.id.expect.json",
                TestData.SECOND_ROW_7_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_7_ID);
    }

    @Test
    void insert_second_int() {
        runTest("put",
                "second/${0}?$select=int",
                "put.insert.second.int.json",
                200,
                "put.insert.second.int.expect.json",
                TestData.SECOND_ROW_8_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_8_ID);
    }

    @Test
    void insert_third_time_all() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_4_NAME + ":" + TestData.THIRD_ROW_4_INDX,
                "put.insert.third.time-all.json",
                200,
                "put.insert.third.time-all.expect.json",
                TestData.THIRD_ROW_1_TIME);
    }

    @Test
    void insert_third_null_all() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_5_NAME + ":" + TestData.THIRD_ROW_5_INDX,
                "put.insert.third.null-all.json",
                200,
                "put.insert.third.null-all.expect.json");
    }

    @Test
    void insert_third_all() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_6_NAME + ":" + TestData.THIRD_ROW_6_INDX,
                "put.insert.third.all.json",
                200,
                "put.insert.third.all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void insert_third_name() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_7_NAME + ":" + TestData.THIRD_ROW_7_INDX + "?$select=name",
                "put.insert.third.name.json",
                200,
                "put.insert.third.name.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void insert_third_bool() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_8_NAME + ":" + TestData.THIRD_ROW_8_INDX + "?$select=bool",
                "put.insert.third.bool.json",
                200,
                "put.insert.third.bool.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void update_first_second_all() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_2_ID,
                "put.update.first.second-all.json",
                200,
                "put.update.first.second-all.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_first_null_all() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_2_ID,
                "put.update.first.null-all.json",
                200,
                "put.update.first.null-all.expect.json");
    }

    @Test
    void update_first_all() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_2_ID,
                "put.update.first.all.json",
                200,
                "put.update.first.all.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_first_id() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_2_ID + "?$select=id",
                "put.update.first.id.json",
                200,
                "put.update.first.id.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_first_varying() {
        runTest("put",
                "first/" + TestData.FIRST_ROW_2_ID + "?$select=varying",
                "put.update.first.varying.json",
                200,
                "put.update.first.varying.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_second_short_all() {
        runTest("put",
                "second/${0}",
                "put.update.second.short-all.json",
                200,
                "put.update.second.short-all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void update_second_null_all() {
        runTest("put",
                "second/${0}",
                "put.update.second.null-all.json",
                200,
                "put.update.second.null-all.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_second_all() {
        runTest("put",
                "second/${0}",
                "put.update.second.all.json",
                200,
                "put.update.second.all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID,
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void update_second_id() {
        runTest("put",
                "second/${0}?$select=id",
                "put.update.second.id.json",
                200,
                "put.update.second.id.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void update_second_int() {
        runTest("put",
                "second/${0}?$select=int",
                "put.update.second.int.json",
                200,
                "put.update.second.int.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void update_third_time_all() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX,
                "put.update.third.time-all.json",
                200,
                "put.update.third.time-all.expect.json",
                TestData.THIRD_ROW_1_TIME,
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void update_third_null_all() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX,
                "put.update.third.null-all.json",
                200,
                "put.update.third.null-all.expect.json");
    }

    @Test
    void update_third_all() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX,
                "put.update.third.all.json",
                200,
                "put.update.third.all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_1_TIME);
    }

    @Test
    void update_third_name() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX + "?$select=name",
                "put.update.third.name.json",
                200,
                "put.update.third.name.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void update_third_bool() {
        runTest("put",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX + "?$select=bool",
                "put.update.third.bool.json",
                200,
                "put.update.third.bool.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }
}
