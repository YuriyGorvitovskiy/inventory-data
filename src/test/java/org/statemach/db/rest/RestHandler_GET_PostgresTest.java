package org.statemach.db.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class RestHandler_GET_PostgresTest extends RestHandler_Common_PostgresTest {

    void runGetTest(String pathTemplate, String expectedTemplateResource, Object... templateParameters) {
        runTest("get", pathTemplate, "empty.json", 200, expectedTemplateResource, templateParameters);
    }

    @Test
    void top() {
        runGetTest("", "get.tables.expect.json");
    }

    @Test
    void first_all() {
        runGetTest("first", "get.first.all.expect.json", TestData.SECOND_ROW_1_ID);
    }

    @Test
    void first_varying() {
        runGetTest("first?varying=Varying%201&$select=id,varying", "get.first.varying.expect.json");
    }

    @Test
    void first_unlimited() {
        runGetTest("first?unlimited=Unlimited%202&$select=id&$select=unlimited", "get.first.unlimited.expect.json");
    }

    @Test
    void first_fixed() {
        runGetTest("first?fixed=Fixed%201&fixed=Fixed%202&$select=id,fixed", "get.first.fixed.expect.json");
    }

    @Test
    void first_search() {
        runGetTest("first?search=Unlimited", "get.first.search.expect.json", TestData.SECOND_ROW_1_ID);
    }

    @Test
    void first_id() {
        runGetTest("first/1", "get.first.id.expect.json", TestData.SECOND_ROW_1_ID);
    }

    @Test
    void second_all() {
        runGetTest("second?$order=int",
                "get.second.all.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_short() {
        runGetTest("second?short=11&$select=id,short", "get.second.short.expect.json", TestData.SECOND_ROW_1_ID);
    }

    @Test
    void second_int() {
        runGetTest("second?int=222&$select=id,int", "get.second.int.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void second_long() {
        runGetTest("second?long=11111&long=22222&$select=id,long&$order=long",
                "get.second.long.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void second_double() {
        runGetTest("second?double=1.2&double=3.4&$order=-double",
                "get.second.double.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_id() {
        runGetTest("second/${0}",
                "get.second.id.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void third_all() {
        runGetTest("third?$order=-indx",
                "get.third.all.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.THIRD_ROW_1_TIME,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_bool() {
        runGetTest("third?bool=true&$select=name,indx,bool",
                "get.third.bool.expect.json");
    }

    @Test
    void third_time() {
        runGetTest("third?&time=${1}&time=${2}&$order=name",
                "get.third.time.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.THIRD_ROW_1_TIME,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_id() {
        runGetTest("third/Name3:2?$select=name,indx&$select=bool,time", "get.third.id.expect.json");
    }

    @Test
    void version() {
        runGetTest("version", "get.version.expect.json", TestDB.version);
    }

}
