package org.statemach.db.graphql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Upsert_PostgresTest extends GraphQLHandler_Common_PostgresTest {

    @BeforeEach
    void restoreRows() {
        TestDB.restoreAllTablesRow2();
    }

    @Test
    void insert_first_second_all() {
        runTest("upsert_insert.first_second-all.gql", "upsert_insert.first_second-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void insert_first_null_all() {
        runTest("upsert_insert.first_null-all.gql", "upsert_insert.first_null-all.expect.json");
    }

    @Test
    void insert_first_all() {
        runTest("upsert_insert.first-all.gql", "upsert_insert.first-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void insert_first_id() {
        runTest("upsert_insert.first-id.gql", "upsert_insert.first-id.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void insert_first_varying() {
        runTest("upsert_insert.first-varying.gql", "upsert_insert.first-varying.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void insert_second_pk_only_all() {
        runTest("upsert_insert.second_short-all.gql",
                "upsert_insert.second_short-all.expect.json",
                TestData.SECOND_ROW_4_ID);
    }

    @Test
    void insert_second_null_all() {
        runTest("upsert_insert.second_null-all.gql", "upsert_insert.second_null-all.expect.json", TestData.SECOND_ROW_5_ID);
    }

    @Test
    void insert_second_all() {
        runTest("upsert_insert.second-all.gql",
                "upsert_insert.second-all.expect.json",
                TestData.SECOND_ROW_6_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_6_ID);
    }

    @Test
    void insert_second_id() {
        runTest("upsert_insert.second-id.gql",
                "upsert_insert.second-id.expect.json",
                TestData.SECOND_ROW_7_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_7_ID);
    }

    @Test
    void insert_second_int() {
        runTest("upsert_insert.second-int.gql",
                "upsert_insert.second-int.expect.json",
                TestData.SECOND_ROW_8_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_8_ID);
    }

    @Test
    void insert_third_time_all() {
        runTest("upsert_insert.third_time-all.gql", "upsert_insert.third_time-all.expect.json", TestData.THIRD_ROW_1_TIME);
    }

    @Test
    void insert_third_null_all() {
        runTest("upsert_insert.third_null-all.gql", "upsert_insert.third_null-all.expect.json");
    }

    @Test
    void insert_third_all() {
        runTest("upsert_insert.third-all.gql",
                "upsert_insert.third-all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void insert_third_name() {
        runTest("upsert_insert.third-name.gql",
                "upsert_insert.third-name.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void insert_third_bool() {
        runTest("upsert_insert.third-bool.gql",
                "upsert_insert.third-bool.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void update_first_second_all() {
        runTest("upsert_update.first_second-all.gql", "upsert_update.first_second-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_first_null_all() {
        runTest("upsert_update.first_null-all.gql", "upsert_update.first_null-all.expect.json");
    }

    @Test
    void update_first_all() {
        runTest("upsert_update.first-all.gql", "upsert_update.first-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_first_id() {
        runTest("upsert_update.first-id.gql", "upsert_update.first-id.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_first_varying() {
        runTest("upsert_update.first-varying.gql", "upsert_update.first-varying.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_second_short_all() {
        runTest("upsert_update.second_short-all.gql",
                "upsert_update.second_short-all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void update_second_null_all() {
        runTest("upsert_update.second_null-all.gql", "upsert_update.second_null-all.expect.json", TestData.SECOND_ROW_2_ID);
    }

    @Test
    void update_second_all() {
        runTest("upsert_update.second-all.gql",
                "upsert_update.second-all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID,
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void update_second_id() {
        runTest("upsert_update.second-id.gql",
                "upsert_update.second-id.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void update_second_int() {
        runTest("upsert_update.second-int.gql",
                "upsert_update.second-int.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void update_third_time_all() {
        runTest("upsert_update.third_time-all.gql",
                "upsert_update.third_time-all.expect.json",
                TestData.THIRD_ROW_1_TIME,
                TestData.SECOND_ROW_1_ID);
    }

    @Test
    void update_third_null_all() {
        runTest("upsert_update.third_null-all.gql", "upsert_update.third_null-all.expect.json");
    }

    @Test
    void update_third_all() {
        runTest("upsert_update.third-all.gql",
                "upsert_update.third-all.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_1_TIME);
    }

    @Test
    void update_third_name() {
        runTest("upsert_update.third-name.gql",
                "upsert_update.third-name.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void update_third_bool() {
        runTest("upsert_update.third-bool.gql",
                "upsert_update.third-bool.expect.json",
                TestData.SECOND_ROW_2_ID,
                TestData.THIRD_ROW_2_TIME);
    }
}
