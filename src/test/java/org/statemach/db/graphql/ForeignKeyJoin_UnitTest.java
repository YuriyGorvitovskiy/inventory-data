package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.sql.Join;

public class ForeignKeyJoin_UnitTest {
    final ForeignKey FOREIGN_KEY_1 = mock(ForeignKey.class);
    final ForeignKey FOREIGN_KEY_2 = mock(ForeignKey.class);

    final ForeignKeyJoin subject = new ForeignKeyJoin(Join.Kind.INNER, FOREIGN_KEY_1, true);
    final ForeignKeyJoin other1  = new ForeignKeyJoin(Join.Kind.INNER, FOREIGN_KEY_1, true);
    final ForeignKeyJoin other2  = new ForeignKeyJoin(Join.Kind.RIGHT, FOREIGN_KEY_1, true);
    final ForeignKeyJoin other3  = new ForeignKeyJoin(Join.Kind.INNER, FOREIGN_KEY_2, true);
    final ForeignKeyJoin other4  = new ForeignKeyJoin(Join.Kind.INNER, FOREIGN_KEY_1, false);

    @Test
    void hashCode_test() {
        // Execute & Verify
        assertEquals(subject.hashCode(), subject.hashCode());
        assertEquals(other1.hashCode(), subject.hashCode());
    }

    @Test
    void equals_test() {
        // Execute & Verify
        assertEquals(subject, subject);
        assertEquals(other1, subject);
        assertNotEquals(other2, subject);
        assertNotEquals(other3, subject);
        assertNotEquals(other4, subject);
    }

    @Test
    void toString_test() {
        // Execute
        String result1 = subject.toString();
        String result2 = other4.toString();

        // Verify
        assertTrue(result1.contains(Join.Kind.INNER.toString()));
        assertTrue(result1.contains(FOREIGN_KEY_1.toString()));
        assertTrue(result1.contains("forward"));

        assertTrue(result2.contains("reverse"));
    }
}
