package org.statemach.db.graphql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestData;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Order_PostgresTest extends GraphQLHandler_Common_PostgresTest {

    @Test
    void first_id() {
        runTest("order.first-id.gql", "order.first-id.expect.json");
    }

    @Test
    void first_fixed() {
        runTest("order.first-fixed.gql", "order.first-fixed.expect.json");
    }

    @Test
    void first_varying() {
        runTest("order.first-varying.gql", "order.first-varying.expect.json");
    }

    @Test
    void first_unlimited() {
        runTest("order.first-unlimited.gql", "order.first-unlimited.expect.json");
    }

    @Test
    void second_long() {
        runTest("order.second-long.gql", "order.second-long.expect.json");
    }

    @Test
    void second_double() {
        runTest("order.second-double.gql", "order.second-double.expect.json");
    }

    @Test
    void second_short() {
        runTest("order.second-short.gql", "order.second-short.expect.json");
    }

    @Test
    void second_int() {
        runTest("order.second-int.gql", "order.second-int.expect.json");
    }

    @Test
    void third_name() {
        runTest("order.third-name.gql", "order.third-name.expect.json");
    }

    @Test
    void third_indx() {
        runTest("order.third-indx.gql", "order.third-indx.expect.json");
    }

    @Test
    void third_bool() {
        runTest("order.third-bool.gql", "order.third-bool.expect.json");
    }

    @Test
    void third_time() {
        runTest("order.third-time.gql", "order.third-time.expect.json", TestData.THIRD_ROW_1_TIME, TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void id_fk_first_second_double() {
        runTest("order.id+fk_first_second-double.gql", "order.id+fk_first_second-double.expect.json");
    }

    @Test
    void id_fk_first_third_time() {
        runTest("order.id+fk_first_third-time.gql", "order.id+fk_first_third-time.expect.json", TestData.THIRD_ROW_1_TIME);
    }

    @Test
    void id_fk_first_second_short_fk_second_third_bool() {
        runTest("order.id+fk_first_second-fk_second_third-bool.gql",
                "order.id+fk_first_second-fk_second_third-bool.expect.json");
    }

    @Test
    void id_fk_first_second_long_fk_first_third_time() {
        runTest("order.id+fk_first_second-long+fk_first_third-time.gql",
                "order.id+fk_first_second-long+fk_first_third-time.expect.json");
    }
}
