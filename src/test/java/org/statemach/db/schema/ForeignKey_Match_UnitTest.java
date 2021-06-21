
package org.statemach.db.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ForeignKey_Match_UnitTest {

    final String COLUMN_NAME_1 = "Column1";
    final String COLUMN_NAME_2 = "Column2";
    final String COLUMN_NAME_3 = "Column3";
    final String COLUMN_NAME_4 = "Column4";

    final ForeignKey.Match subject = new ForeignKey.Match(COLUMN_NAME_1, COLUMN_NAME_2);
    final ForeignKey.Match other1  = new ForeignKey.Match(COLUMN_NAME_1, COLUMN_NAME_2);
    final ForeignKey.Match other2  = new ForeignKey.Match(COLUMN_NAME_3, COLUMN_NAME_2);
    final ForeignKey.Match other3  = new ForeignKey.Match(COLUMN_NAME_1, COLUMN_NAME_4);
    final ForeignKey.Match other4  = new ForeignKey.Match(COLUMN_NAME_3, COLUMN_NAME_4);

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
}
