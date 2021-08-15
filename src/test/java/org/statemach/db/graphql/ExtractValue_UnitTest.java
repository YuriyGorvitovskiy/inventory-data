package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.statemach.db.schema.DataType;

import io.vavr.collection.List;

public class ExtractValue_UnitTest {
    final String NAME_1 = "name_1";
    final String NAME_2 = "name_2";

    final String STEP_1 = "step_1";
    final String STEP_2 = "step_2";
    final String STEP_3 = "step_3";

    final List<String> PATH_1 = List.of(STEP_1, STEP_2);
    final List<String> PATH_2 = List.of(STEP_1, STEP_3);

    final DataType TYPE_1 = mock(DataType.class);
    final DataType TYPE_2 = mock(DataType.class);

    final ExtractValue subject = new ExtractValue(NAME_1, PATH_1, TYPE_1);
    final ExtractValue other1  = new ExtractValue(NAME_1, PATH_1, TYPE_1);
    final ExtractValue other2  = new ExtractValue(NAME_2, PATH_1, TYPE_1);
    final ExtractValue other3  = new ExtractValue(NAME_1, PATH_2, TYPE_1);
    final ExtractValue other4  = new ExtractValue(NAME_1, PATH_1, TYPE_2);

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
        String result = subject.toString();

        // Verify
        assertTrue(result.contains(NAME_1));
        assertTrue(result.contains(STEP_1));
        assertTrue(result.contains(STEP_2));
        assertTrue(result.contains(TYPE_1.toString()));
    }
}
