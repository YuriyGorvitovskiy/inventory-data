package org.statemach.db.graphql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Update_PostgresTest extends GraphQLHandler_Common_PostgresTest {

    @BeforeEach
    void restoreRows() {
        TestDB.restoreAllTablesRow2();
    }

    @Test
    void first_second_all() {
        runTest("update.first_second-all.gql", "update.first_second-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_null_all() {
        runTest("update.first_null-all.gql", "update.first_null-all.expect.json");
    }

    @Test
    void first_all() {
        runTest("update.first-all.gql", "update.first-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_id() {
        runTest("update.first-id.gql", "update.first-id.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_varying() {
        runTest("update.first-varying.gql", "update.first-varying.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void second_short_all() {
        runTest("update.second_short-all.gql",
                "update.second_short-all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_null_all() {
        runTest("update.second_null-all.gql", "update.second_null-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void second_all() {
        runTest("update.second-all.gql",
                "update.second-all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID,
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void second_id() {
        runTest("update.second-id.gql",
                "update.second-id.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void second_int() {
        runTest("update.second-int.gql",
                "update.second-int.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void third_time_all() {
        runTest("update.third_time-all.gql",
                "update.third_time-all.expect.json",
                TestData.THIRD_ROW_1_TIME,
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void third_null_all() {
        runTest("update.third_null-all.gql", "update.third_null-all.expect.json");
    }

    @Test
    void third_all() {
        runTest("update.third-all.gql", "update.third-all.expect.json", TestData.SECOND_ROW_2_ID, TestData.THIRD_ROW_1_TIME);
    }

    @Test
    void third_name() {
        runTest("update.third-name.gql", "update.third-name.expect.json", TestData.SECOND_ROW_2_ID, TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_bool() {
        runTest("update.third-bool.gql", "update.third-bool.expect.json", TestData.SECOND_ROW_2_ID, TestData.THIRD_ROW_2_TIME);
    }

}
