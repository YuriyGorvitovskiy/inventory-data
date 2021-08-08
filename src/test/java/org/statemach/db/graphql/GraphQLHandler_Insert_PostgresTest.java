package org.statemach.db.graphql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestData;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Insert_PostgresTest extends GraphQLHandler_Common_PostgresTest {

    @Test
    void first_pk_only_all() {
        runTest("insert.first_pk_only-all.gql", "insert.first_pk_only-all.expect.json");
    }

    @Test
    void first_null_all() {
        runTest("insert.first_null-all.gql", "insert.first_null-all.expect.json");
    }

    @Test
    void first_all() {
        runTest("insert.first-all.gql", "insert.first-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_id() {
        runTest("insert.first-id.gql", "insert.first-id.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void first_varying() {
        runTest("insert.first-varying.gql", "insert.first-varying.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void second_pk_only_all() {
        runTest("insert.second_pk_only-all.gql", "insert.second_pk_only-all.expect.json", TestData.SECOND_ROW_4_ID);
    }

    @Test
    void second_null_all() {
        runTest("insert.second_null-all.gql", "insert.second_null-all.expect.json", TestData.SECOND_ROW_5_ID);
    }

    @Test
    void second_all() {
        runTest("insert.second-all.gql",
                "insert.second-all.expect.json",
                TestData.SECOND_ROW_6_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_6_ID);
    }

    @Test
    void second_id() {
        runTest("insert.second-id.gql",
                "insert.second-id.expect.json",
                TestData.SECOND_ROW_7_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_7_ID);
    }

    @Test
    void second_int() {
        runTest("insert.second-int.gql",
                "insert.second-int.expect.json",
                TestData.SECOND_ROW_8_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_8_ID);
    }

    @Test
    void third_pk_only_all() {
        runTest("insert.third_pk_only-all.gql", "insert.third_pk_only-all.expect.json");
    }

    @Test
    void third_null_all() {
        runTest("insert.third_null-all.gql", "insert.third_null-all.expect.json");
    }

    @Test
    void third_all() {
        runTest("insert.third-all.gql", "insert.third-all.expect.json", TestData.SECOND_ROW_2_ID, TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_name() {
        runTest("insert.third-name.gql", "insert.third-name.expect.json", TestData.SECOND_ROW_2_ID, TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void third_bool() {
        runTest("insert.third-bool.gql", "insert.third-bool.expect.json", TestData.SECOND_ROW_2_ID, TestData.THIRD_ROW_2_TIME);
    }

}
