package org.statemach.db.sql.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.jdbc.Vendor;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.Select;

public class PostgresSQLBuilder_UnitTest {

    PostgresSQLBuilder subject = new PostgresSQLBuilder(TestDB.schema);

    @Test
    void getVendor() {
        // Execute
        Vendor result = subject.getVendor();

        //Setup
        assertEquals(Vendor.POSTGRES, result);
    }

    @Test
    void and_empty() {
        // Execute
        Condition result = subject.and();

        // Verify
        assertSame(Condition.NONE, result);
    }

    @Test
    void and_single() {
        // Setup
        Condition some = subject.equal(Select.of("t", "column", null), Inject.STRING.apply("value"));

        // Execute
        Condition result = subject.and(some);

        // Verify
        assertSame(some, result);
    }

    @Test
    void or_empty() {
        // Execute
        Condition result = subject.or();

        // Verify
        assertSame(Condition.NONE, result);
    }

    @Test
    void or_single() {
        // Setup
        Condition some = subject.equal(Select.of("t", "column", null), Inject.STRING.apply("value"));

        // Execute
        Condition result = subject.or(some);

        // Verify
        assertSame(some, result);
    }

}
