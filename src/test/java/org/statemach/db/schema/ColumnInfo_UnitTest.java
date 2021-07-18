package org.statemach.db.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

public class ColumnInfo_UnitTest {

    final String   COLUMN_NAME_1 = "Column1";
    final String   COLUMN_NAME_2 = "Column2";
    final DataType DATA_TYPE_1   = mock(DataType.class);
    final DataType DATA_TYPE_2   = mock(DataType.class);

    final ColumnInfo subject = new ColumnInfo(COLUMN_NAME_1, DATA_TYPE_1);
    final ColumnInfo other1  = new ColumnInfo(COLUMN_NAME_1, DATA_TYPE_1);
    final ColumnInfo other2  = new ColumnInfo(COLUMN_NAME_1, DATA_TYPE_2);
    final ColumnInfo other3  = new ColumnInfo(COLUMN_NAME_2, DATA_TYPE_1);
    final ColumnInfo other4  = new ColumnInfo(COLUMN_NAME_2, DATA_TYPE_2);

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

        // Verify
        assertTrue(result);
        assertTrue(result1);
        assertFalse(result2);
        assertFalse(result3);
        assertFalse(result4);
    }

    @Test
    void toString_test() {
        // Execute
        String result = subject.toString();

        // Verify
        assertTrue(result.contains(COLUMN_NAME_1));
        assertTrue(result.contains(DATA_TYPE_1.toString()));
    }
}
