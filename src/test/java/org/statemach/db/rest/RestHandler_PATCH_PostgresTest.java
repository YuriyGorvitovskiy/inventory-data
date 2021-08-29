package org.statemach.db.rest;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;
import org.statemach.util.Http;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class RestHandler_PATCH_PostgresTest extends RestHandler_Common_PostgresTest {

    @BeforeEach
    void restoreRows() {
        TestDB.restoreAllTablesRow2();
    }

    @Test
    void first_second_all() {
        runTest("patch",
                "first/" + TestData.FIRST_ROW_2_ID,
                "patch.first.second-all.json",
                200,
                "patch.first.second-all.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_null_all() {
        runTest("patch",
                "first/" + TestData.FIRST_ROW_2_ID,
                "patch.first.null-all.json",
                200,
                "patch.first.null-all.expect.json");
    }

    @Test
    void first_all() {
        runTest("patch",
                "first/" + TestData.FIRST_ROW_2_ID,
                "patch.first.all.json",
                200,
                "patch.first.all.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_id() {
        runTest("patch",
                "first/" + TestData.FIRST_ROW_2_ID + "?$select=id",
                "patch.first.id.json",
                200,
                "patch.first.id.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_varying() {
        runTest("patch",
                "first/" + TestData.FIRST_ROW_2_ID + "?$select=varying",
                "patch.first.varying.json",
                200,
                "patch.first.varying.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_not_exists() {
        assertThrows(Http.Error.class,
                () -> runTest("patch", "first/22", "patch.first.not-exists.json", 404, "empty.json"));
    }

    @Test
    void second_short_all() {
        runTest("patch",
                "second/${0}",
                "patch.second.short-all.json",
                200,
                "patch.second.short-all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_null_all() {
        runTest("patch",
                "second/${0}",
                "patch.second.null-all.json",
                200,
                "patch.second.null-all.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void second_all() {
        runTest("patch",
                "second/${0}",
                "patch.second.all.json",
                200,
                "patch.second.all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID,
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void second_id() {
        runTest("patch",
                "second/${0}?$select=id",
                "patch.second.id.json",
                200,
                "patch.second.id.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_int() {
        runTest("patch",
                "second/${0}?$select=int",
                "patch.second.int.json",
                200,
                "patch.second.int.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_not_exists() {
        UUID uuid = UUID.randomUUID();
        assertThrows(Http.Error.class,
                () -> runTest("patch", "second/${0}", "patch.second.not-exists.json", 404, "empty.json", uuid));
    }

    @Test
    void third_time_all() {
        runTest("patch",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX,
                "patch.third.time-all.json",
                200,
                "patch.third.time-all.expect.json",
                TestData.THIRD_ROW_1_TIME,
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void third_null_all() {
        runTest("patch",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX,
                "patch.third.null-all.json",
                200,
                "patch.third.null-all.expect.json");
    }

    @Test
    void third_all() {
        runTest("patch",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX,
                "patch.third.all.json",
                200,
                "patch.third.all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_1_TIME);
    }

    @Test
    void third_name() {
        runTest("patch",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX + "?$select=name",
                "patch.third.name.json",
                200,
                "patch.third.name.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_bool() {
        runTest("patch",
                "third/" + TestData.THIRD_ROW_2_NAME + ":" + TestData.THIRD_ROW_2_INDX + "?$select=bool",
                "patch.third.bool.json",
                200,
                "patch.third.bool.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_not_exists() {
        assertThrows(Http.Error.class,
                () -> runTest("patch", "third/Hello:2", "patch.third.not-exists.json", 404, "empty.json"));
    }

}
