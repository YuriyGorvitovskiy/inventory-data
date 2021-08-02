package org.statemach.db.graphql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestData;
import org.statemach.util.Json;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Filter_PostgresTest extends GraphQLHandler_Common_PostgresTest {

    @Test
    void first_id() {
        runTest("filter.first-id.gql", "filter.first-id.expect.json");
    }

    @Test
    void first_second() {
        runTest("filter.first-second.gql", "filter.first-second.expect.json");
    }

    @Test
    void first_third_name() {
        runTest("filter.first-third_name.gql", "filter.first-third_name.expect.json");
    }

    @Test
    void first_third_indx() {
        runTest("filter.first-third_indx.gql", "filter.first-third_indx.expect.json");
    }

    @Test
    void first_fixed() {
        runTest("filter.first-fixed.gql", "filter.first-fixed.expect.json");
    }

    @Test
    void first_varying() {
        runTest("filter.first-varying.gql", "filter.first-varying.expect.json");
    }

    @Test
    void first_unlimited() {
        runTest("filter.first-unlimited.gql", "filter.first-unlimited.expect.json");
    }

    @Test
    void first_search() {
        runTest("filter.first-search.gql", "filter.first-search.expect.json");
    }

    @Test
    void fk_first_second_double() {
        runTest("filter.fk_first_second-double.gql", "filter.fk_first_second-double.expect.json");
    }

    @Test
    void fk_first_second_first() {
        runTest("filter.fk_first_second-first.gql", "filter.fk_first_second-first.expect.json");
    }

    @Test
    void fk_first_second_int() {
        runTest("filter.fk_first_second-int.gql", "filter.fk_first_second-int.expect.json");
    }

    @Test
    void fk_second_first_reverse_id() {
        runTest("filter.fk_second_first_reverse-id.gql",
                "filter.fk_second_first_reverse-id.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void fk_second_first_reverse_long() {
        runTest("filter.fk_second_first_reverse-long.gql", "filter.fk_second_first_reverse-long.expect.json");
    }

    @Test
    void fk_second_first_reverse_short() {
        runTest("filter.fk_second_first_reverse-short.gql", "filter.fk_second_first_reverse-short.expect.json");
    }

    @Test
    void fk_first_second_fk_second_third_bool() {
        runTest("filter.fk_first_second-fk_second_third-bool.gql", "filter.fk_first_second-fk_second_third-bool.expect.json");
    }

    @Test
    void fk_first_second_fk_third_second_reverse_bool() {
        runTest("filter.fk_first_second-fk_third_second_reverse-bool.gql",
                "filter.fk_first_second-fk_third_second_reverse-bool.expect.json");
    }

    @Test
    void fk_second_first_reverse_fk_second_third_time() {
        runTest("filter.fk_second_first_reverse-fk_second_third-time.gql",
                "filter.fk_second_first_reverse-fk_second_third-time.expect.json",
                Json.toISO8601(TestData.THIRD_ROW_1_TIME));
    }

    @Test
    void fk_second_first_reverse_fk_third_second_reverse_time() {
        runTest("filter.fk_second_first_reverse-fk_third_second_reverse-time.gql",
                "filter.fk_second_first_reverse-fk_third_second_reverse-time.expect.json",
                Json.toISO8601(TestData.THIRD_ROW_1_TIME),
                Json.toISO8601(TestData.THIRD_ROW_2_TIME));
    }

    @Test
    void fk_first_second_int_fk_first_third_bool() {
        runTest("filter.fk_first_second-int+fk_first_third-bool.gql",
                "filter.fk_first_second-int+fk_first_third-bool.expect.json");
    }

    @Test
    void fk_first_second_int_fk_third_first_reverse_bool() {
        runTest("filter.fk_first_second-int+fk_third_first_reverse-bool.gql",
                "filter.fk_first_second-int+fk_third_first_reverse-bool.expect.json");
    }

    @Test
    void fk_second_first_reverse_int_fk_first_third_indx() {
        runTest("filter.fk_second_first_reverse-int+fk_first_third-indx.gql",
                "filter.fk_second_first_reverse-int+fk_first_third-indx.expect.json");
    }

    @Test
    void fk_second_first_reverse_int_fk_third_first_reverse_indx() {
        runTest("filter.fk_second_first_reverse-int+fk_third_first_reverse-indx.gql",
                "filter.fk_second_first_reverse-int+fk_third_first_reverse-indx.expect.json");
    }
}
