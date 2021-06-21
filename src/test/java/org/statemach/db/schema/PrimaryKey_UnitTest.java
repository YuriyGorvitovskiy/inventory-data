package org.statemach.db.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.vavr.collection.List;

public class PrimaryKey_UnitTest {

    final String PRIMARY_KEY_NAME_1 = "PrimaryKey1";
    final String PRIMARY_KEY_NAME_2 = "PrimaryKey2";

    final String TABLE_NAME_1 = "Table1";
    final String TABLE_NAME_2 = "Table2";

    final String COLUMN_NAME_1 = "Column1";
    final String COLUMN_NAME_2 = "Column2";
    final String COLUMN_NAME_3 = "Column3";

    final PrimaryKey subject = new PrimaryKey(PRIMARY_KEY_NAME_1, TABLE_NAME_1, List.of(COLUMN_NAME_1, COLUMN_NAME_2));
    final PrimaryKey other1  = new PrimaryKey(PRIMARY_KEY_NAME_1, TABLE_NAME_1, List.of(COLUMN_NAME_1, COLUMN_NAME_2));
    final PrimaryKey other2  = new PrimaryKey(PRIMARY_KEY_NAME_2, TABLE_NAME_1, List.of(COLUMN_NAME_1, COLUMN_NAME_2));
    final PrimaryKey other3  = new PrimaryKey(PRIMARY_KEY_NAME_1, TABLE_NAME_2, List.of(COLUMN_NAME_1, COLUMN_NAME_2));
    final PrimaryKey other4  = new PrimaryKey(PRIMARY_KEY_NAME_1, TABLE_NAME_1, List.of(COLUMN_NAME_1, COLUMN_NAME_3));
    final PrimaryKey other5  = new PrimaryKey(PRIMARY_KEY_NAME_1, TABLE_NAME_1, List.of(COLUMN_NAME_2, COLUMN_NAME_1));
    final PrimaryKey other6  = new PrimaryKey(PRIMARY_KEY_NAME_1, TABLE_NAME_1, List.of(COLUMN_NAME_1));

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
        boolean result3 = subject.equals(other3);
        boolean result4 = subject.equals(other4);
        boolean result5 = subject.equals(other5);
        boolean result6 = subject.equals(other6);

        // Verify
        assertTrue(result);
        assertTrue(result1);
        assertFalse(result2);
        assertFalse(result3);
        assertFalse(result4);
        assertFalse(result5);
        assertFalse(result6);
    }
}
