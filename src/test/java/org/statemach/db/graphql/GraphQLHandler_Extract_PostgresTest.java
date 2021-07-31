package org.statemach.db.graphql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;
import org.statemach.util.Json;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Extract_PostgresTest extends GraphQLHandler_Common_PostgresTest {

    @Test
    void first() {
        runTest("extract.first.gql", "extract.first.expect.json", TestData.SECOND_ROW_1_ID);
    }

    @Test
    void second() {
        runTest("extract.second.gql",
                "extract.second.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void third() {
        runTest("extract.third.gql",
                "extract.third.expect.json",
                TestData.SECOND_ROW_1_ID,
                Json.toISO8601(TestData.THIRD_ROW_1_TIME),
                Json.toISO8601(TestData.THIRD_ROW_2_TIME));
    }

    @Test
    void version() {
        runTest("extract.version.gql", "extract.version.expect.json", TestDB.product, TestDB.version);
    }

    @Test
    void fk_first_second() {
        runTest("extract.fk_first_second.gql",
                "extract.fk_first_second.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void fk_first_second_reverse() {
        runTest("extract.fk_first_second_reverse.gql",
                "extract.fk_first_second_reverse.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void fk_first_third() {
        runTest("extract.fk_first_third.gql",
                "extract.fk_first_third.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.THIRD_ROW_1_TIME);
    }

    @Test
    void fk_first_third_reverse() {
        runTest("extract.fk_first_third_reverse.gql",
                "extract.fk_first_third_reverse.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.THIRD_ROW_1_TIME,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void fk_second_one_fk_second_two() {
        runTest("extract.fk_second_one+fk_second_two.gql",
                "extract.fk_second_one+fk_second_two.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void fk_third_first_fk_third_second() {
        runTest("extract.fk_third_first+fk_third_second.gql",
                "extract.fk_third_first+fk_third_second.expect.json",
                TestData.THIRD_ROW_1_TIME,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void fk_first_third_reverse_fk_second_third_reverse() {
        runTest("extract.fk_first_third_reverse+fk_second_third_reverse.gql",
                "extract.fk_first_third_reverse+fk_second_third_reverse.expect.json",
                TestData.THIRD_ROW_1_TIME,
                TestData.THIRD_ROW_2_TIME);
    }

    @Test
    void fk_second_one_reverse_fk_second_two_reverse() {
        runTest("extract.fk_second_one_reverse-fk_second_two_reverse.gql",
                "extract.fk_second_one_reverse-fk_second_two_reverse.expect.json");
    }

    @Test
    void fk_second_first_reverse_fk_third_second_reverse() {
        runTest("extract.fk_second_first_reverse-fk_third_second_reverse.gql",
                "extract.fk_second_first_reverse-fk_third_second_reverse.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID);
    }

    @Test
    void fk_second_one_fk_second_two_reverse() {
        runTest("extract.fk_second_one-fk_second_two_reverse.gql",
                "extract.fk_second_one-fk_second_two_reverse.expect.json");
    }

    @Test
    void fk_second_one_reverse_fk_second_two() {
        runTest("extract.fk_second_one_reverse-fk_second_two.gql",
                "extract.fk_second_one_reverse-fk_second_two.expect.json");
    }

    @Test
    void fk_first_second_fk_third_second_reverse_fk_first_second_reverse() {
        runTest("extract.fk_first_second-fk_third_second_reverse+fk_first_second_reverse.gql",
                "extract.fk_first_second-fk_third_second_reverse+fk_first_second_reverse.expect.json");
    }

}
