package org.statemach.db.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestData;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class RestHandler_POST_PostgresTest extends RestHandler_Common_PostgresTest {

    @Test
    void first_pk_only_all() {
        runTest("post", "first", "post.first.pk_only-all.json", 200, "post.first.pk_only-all.expect.json");
    }

    @Test
    void first_null_all() {
        runTest("post", "first", "post.first.null-all.json", 200, "post.first.null-all.expect.json");
    }

    @Test
    void first_all() {
        runTest("post", "first", "post.first.all.json", 200, "post.first.all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_id() {
        runTest("post", "first?$select=id", "post.first.id.json", 200, "post.first.id.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_varying() {
        runTest("post",
                "first?$select=varying",
                "post.first.varying.json",
                200,
                "post.first.varying.expect.json",
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void second_pk_only_all() {
        runTest("post",
                "second",
                "post.second.pk_only-all.json",
                200,
                "post.second.pk_only-all.expect.json",
                TestData.SECOND_ROW_4_ID);
    }

    @Test
    void second_null_all() {
        runTest("post",
                "second",
                "post.second.null-all.json",
                200,
                "post.second.null-all.expect.json",
                TestData.SECOND_ROW_5_ID);
    }

    @Test
    void second_all() {
        runTest("post",
                "second",
                "post.second.all.json",
                200,
                "post.second.all.expect.json",
                TestData.SECOND_ROW_6_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_6_ID);
    }

    @Test
    void second_id() {
        runTest("post",
                "second?$select=id",
                "post.second.id.json",
                200,
                "post.second.id.expect.json",
                TestData.SECOND_ROW_7_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_7_ID);
    }

    @Test
    void second_int() {
        runTest("post",
                "second?$select=int",
                "post.second.int.json",
                200,
                "post.second.int.expect.json",
                TestData.SECOND_ROW_8_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_8_ID);
    }

    @Test
    void third_pk_only_all() {
        runTest("post", "third", "post.third.pk_only-all.json", 200, "post.third.pk_only-all.expect.json");
    }

    @Test
    void third_null_all() {
        runTest("post", "third", "post.third.null-all.json", 200, "post.third.null-all.expect.json");
    }

    @Test
    void third_all() {
        runTest("post",
                "third",
                "post.third.all.json",
                200,
                "post.third.all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_name() {
        runTest("post",
                "third?$select=name",
                "post.third.name.json",
                200,
                "post.third.name.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_bool() {
        runTest("post",
                "third?$select=bool",
                "post.third.bool.json",
                200,
                "post.third.bool.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

}
