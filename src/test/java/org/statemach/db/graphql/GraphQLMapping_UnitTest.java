package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.statemach.db.jdbc.Vendor;

public class GraphQLMapping_UnitTest {

    @Test
    void of_POSTGRESS() {
        // Execute
        GraphQLMapping result = GraphQLMapping.of(Vendor.POSTGRES);

        // Verify
        assertSame(GraphQLMapping.POSTGRES_TO_SCALAR, result.scalars);
    }

    @Test
    void of_null() {
        // Execute & Verify
        assertThrows(RuntimeException.class, () -> GraphQLMapping.of(null));
    }
}
