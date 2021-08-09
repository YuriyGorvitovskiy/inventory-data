package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.vavr.collection.List;

public class OrderBy_UnitTest {
    static final String STEP_1 = "first";
    static final String STEP_2 = "second";
    static final String STEP_3 = "third";

    static final List<String> PATH_1 = List.of(STEP_1, STEP_2);
    static final List<String> PATH_2 = List.of(STEP_1, STEP_3);

    final OrderBy subject1 = new OrderBy(PATH_1, true);
    final OrderBy subject2 = new OrderBy(PATH_1, true);
    final OrderBy subject3 = new OrderBy(PATH_2, true);
    final OrderBy subject4 = new OrderBy(PATH_1, false);

    @Test
    void _hashCode() {
        // Execute
        int result1 = subject1.hashCode();
        int result2 = subject2.hashCode();

        // Verify
        assertEquals(result1, result2);
    }

    @Test
    void _equals() {
        // Execute
        boolean result1 = subject1.equals(subject1);
        boolean result2 = subject1.equals(subject2);
        boolean result3 = subject1.equals(subject3);
        boolean result4 = subject1.equals(subject4);

        // Verify
        assertTrue(result1);
        assertTrue(result2);
        assertFalse(result3);
        assertFalse(result4);
    }

    @Test
    void _toString() {
        // Execute
        String result1 = subject1.toString();

        // Verify
        assertTrue(result1.contains(STEP_1));
        assertTrue(result1.contains(STEP_2));
        assertTrue(result1.contains(Boolean.TRUE.toString()));
    }
}
