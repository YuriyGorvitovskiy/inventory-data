package org.statemach.db.rest;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;
import org.statemach.util.Http;

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
    void table_not_exists() {
        assertThrows(Http.Error.class,
                () -> runTest("get", "other", "empty.json", 400, "empty.json"));
    }

    @Test
    void table_id_not_exists() {
        assertThrows(Http.Error.class,
                () -> runTest("get", "other/22", "empty.json", 400, "empty.json"));
    }

    @Test
    void first_all() {
        runGetTest("first", "get.first.all.expect.json", TestData.SECOND_ROW_1_ID);
    }

    @Test
    void first_skip_limit() {
        runGetTest("first?$skip=1&$limit=1", "get.first.skip-limit.expect.json", TestData.SECOND_ROW_1_ID);
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
    void first_select_not_extractable() {
        assertThrows(Http.Error.class,
                () -> runGetTest("first?$select=id,search", "empty.json"));
    }

    @Test
    void first_id() {
        runGetTest("first/1", "get.first.id.expect.json", TestData.SECOND_ROW_1_ID);
    }

    @Test
    void first_id_not_exists() {
        assertThrows(Http.Error.class,
                () -> runGetTest("first/22", "empty.json"));
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
        runGetTest("second?short=11&short=12&short=13&short=13&short=14&short=15&short=16&short=17&short=18&$select=id,short",
                "get.second.short.expect.json",
                TestData.SECOND_ROW_1_ID);
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
    void second_order_not_exists() {
        assertThrows(Http.Error.class,
                () -> runGetTest("third?$order=id,other", "empty.json"));
    }

    @Test
    void second_id() {
        runGetTest("second/${0}",
                "get.second.id.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_id_not_exists() {
        UUID uuid = UUID.randomUUID();
        assertThrows(Http.Error.class,
                () -> runGetTest("second/${0}", "empty.json", uuid));
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
    void third_select_not_exists() {
        assertThrows(Http.Error.class,
                () -> runGetTest("third?$select=id,other", "empty.json"));
    }

    @Test
    void third_filter_not_exists() {
        assertThrows(Http.Error.class,
                () -> runGetTest("third?$select=id&other=2", "empty.json"));
    }

    @Test
    void third_filter_unsupported() {
        assertThrows(Http.Error.class,
                () -> runGetTest("third?$select=id&unsupported=12", "empty.json"));
    }

    @Test
    void third_id() {
        runGetTest("third/Name3:2?$select=name,indx&$select=bool,time", "get.third.id.expect.json");
    }

    @Test
    void third_id_missing_part() {
        assertThrows(Http.Error.class,
                () -> runGetTest("third/Name3", "empty.json"));
    }

    @Test
    void third_id_not_exists() {
        assertThrows(Http.Error.class,
                () -> runGetTest("third/Name3:5", "empty.json"));
    }

    @Test
    void version() {
        runGetTest("version", "get.version.expect.json", TestDB.version);
    }

    @Test
    void version_id() {
        assertThrows(Http.Error.class, () -> runGetTest("version/something", "empty.json"));
    }
}
