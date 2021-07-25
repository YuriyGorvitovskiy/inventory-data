package org.statemach.db.graphql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.statemach.db.sql.postgres.TestDB;
import org.statemach.db.sql.postgres.TestData;
import org.statemach.util.Json;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Extract_PostgresTest extends GraphQLHandler_Common_PostgresTest {

    @Test
    void firstOnly() {
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
}
