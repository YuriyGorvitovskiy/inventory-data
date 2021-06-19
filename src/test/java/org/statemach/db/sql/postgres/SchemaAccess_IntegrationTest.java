package org.statemach.db.sql.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class SchemaAccess_IntegrationTest {

    @Test
    void createSchema() {
        TestDB.setup();
    }
}
