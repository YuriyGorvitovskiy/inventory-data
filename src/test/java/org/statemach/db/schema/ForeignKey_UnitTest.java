package org.statemach.db.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.vavr.collection.List;

public class ForeignKey_UnitTest {

    final String FOREIGN_KEY_NAME_1 = "ForeignKey1";
    final String FOREIGN_KEY_NAME_2 = "ForeignKey2";

    final String TABLE_NAME_1 = "Table1";
    final String TABLE_NAME_2 = "Table2";
    final String TABLE_NAME_3 = "Table3";
    final String TABLE_NAME_4 = "Table4";

    final String COLUMN_NAME_1 = "Column1";
    final String COLUMN_NAME_2 = "Column2";
    final String COLUMN_NAME_3 = "Column3";
    final String COLUMN_NAME_4 = "Column4";

    final ForeignKey.Match MATCH_1 = new ForeignKey.Match(COLUMN_NAME_1, COLUMN_NAME_2);
    final ForeignKey.Match MATCH_2 = new ForeignKey.Match(COLUMN_NAME_3, COLUMN_NAME_4);
    final ForeignKey.Match MATCH_3 = new ForeignKey.Match(COLUMN_NAME_2, COLUMN_NAME_1);

    final ForeignKey subject = new ForeignKey(FOREIGN_KEY_NAME_1, TABLE_NAME_1, TABLE_NAME_2, List.of(MATCH_1, MATCH_2));
    final ForeignKey other1  = new ForeignKey(FOREIGN_KEY_NAME_1, TABLE_NAME_1, TABLE_NAME_2, List.of(MATCH_1, MATCH_2));
    final ForeignKey other2  = new ForeignKey(FOREIGN_KEY_NAME_2, TABLE_NAME_1, TABLE_NAME_2, List.of(MATCH_1, MATCH_2));
    final ForeignKey other3  = new ForeignKey(FOREIGN_KEY_NAME_1, TABLE_NAME_3, TABLE_NAME_2, List.of(MATCH_1, MATCH_2));
    final ForeignKey other4  = new ForeignKey(FOREIGN_KEY_NAME_1, TABLE_NAME_1, TABLE_NAME_4, List.of(MATCH_1, MATCH_2));
    final ForeignKey other5  = new ForeignKey(FOREIGN_KEY_NAME_1, TABLE_NAME_1, TABLE_NAME_2, List.of(MATCH_2, MATCH_1));
    final ForeignKey other6  = new ForeignKey(FOREIGN_KEY_NAME_1, TABLE_NAME_1, TABLE_NAME_2, List.of(MATCH_3, MATCH_2));
    final ForeignKey other7  = new ForeignKey(FOREIGN_KEY_NAME_1, TABLE_NAME_1, TABLE_NAME_2, List.of(MATCH_1, MATCH_3));
    final ForeignKey other8  = new ForeignKey(FOREIGN_KEY_NAME_1, TABLE_NAME_1, TABLE_NAME_2, List.of(MATCH_1));

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
        boolean result7 = subject.equals(other7);
        boolean result8 = subject.equals(other8);

        // Verify
        assertTrue(result);
        assertTrue(result1);
        assertFalse(result2);
        assertFalse(result3);
        assertFalse(result4);
        assertFalse(result5);
        assertFalse(result6);
        assertFalse(result7);
        assertFalse(result8);
    }
}
