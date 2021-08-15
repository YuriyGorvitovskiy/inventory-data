package org.statemach.db.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import io.vavr.collection.List;

public class CompositeType_UnitTest {
    final String NAME_1 = "Type1";
    final String NAME_2 = "Type2";

    final ColumnInfo COLUMN_1 = mock(ColumnInfo.class);
    final ColumnInfo COLUMN_2 = mock(ColumnInfo.class);
    final ColumnInfo COLUMN_3 = mock(ColumnInfo.class);

    final List<ColumnInfo> FILEDS_1 = List.of(COLUMN_1, COLUMN_2);
    final List<ColumnInfo> FILEDS_2 = List.of(COLUMN_1, COLUMN_3);

    final CompositeType subject = new CompositeType(NAME_1, FILEDS_1);
    final CompositeType other1  = new CompositeType(NAME_1, FILEDS_1);
    final CompositeType other2  = new CompositeType(NAME_2, FILEDS_1);
    final CompositeType other3  = new CompositeType(NAME_1, FILEDS_2);

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

        // Verify
        assertTrue(result);
        assertTrue(result1);
        assertFalse(result2);
        assertFalse(result3);
    }

    @Test
    void toString_test() {
        // Execute
        String result = subject.toString();

        // Verify
        assertTrue(result.contains(NAME_1));
    }

}
