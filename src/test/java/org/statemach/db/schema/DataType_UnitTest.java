package org.statemach.db.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class DataType_UnitTest {

    final String TYPE_NAME_1 = "DataType1";
    final String TYPE_NAME_2 = "DataType2";

    final DataType subject = new DataType(TYPE_NAME_1);
    final DataType other1  = new DataType(TYPE_NAME_1);
    final DataType other2  = new DataType(TYPE_NAME_2);

    @Test
    void unsupported() {
        // Execute
        DataType result = DataType.unsupported(TYPE_NAME_1);

        // Verify
        assertEquals(subject, result);
    }

    @Test
    void hashCode_test() {
        // Execute
        int result  = subject.hashCode();
        int result1 = other1.hashCode();

        // Verify
        assertEquals(result, result1);
    }

    @Test
    void equals_test() {
        // Execute
        boolean result  = subject.equals(subject);
        boolean result1 = subject.equals(other1);
        boolean result2 = subject.equals(other2);

        // Verify
        assertTrue(result);
        assertTrue(result1);
        assertFalse(result2);
    }

    @Test
    void toString_test() {
        // Execute
        String result = subject.toString();

        // Verify
        assertEquals("DataType@{name: 'DataType1'}", result);
    }
}
