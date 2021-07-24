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
        runTest("extract.first-only.gql", "extract.first-only.expect.json", TestData.SECOND_ROW_1_ID);
    }

    @Test
    void secondOnly() {
        runTest("extract.second-only.gql",
                "extract.second-only.expect.json",
                TestData.SECOND_ROW_1_ID,
                TestData.SECOND_ROW_2_ID,
                TestData.SECOND_ROW_3_ID);
    }

    @Test
    void thirdOnly() {
        runTest("extract.third-only.gql",
                "extract.third-only.expect.json",
                TestData.SECOND_ROW_1_ID,
                Json.toISO8601(TestData.THIRD_ROW_2_TIME));
    }

    @Test
    void versionOnly() {
        runTest("extract.version-only.gql", "extract.version-only.expect.json", TestDB.product, TestDB.version);
    }
}
