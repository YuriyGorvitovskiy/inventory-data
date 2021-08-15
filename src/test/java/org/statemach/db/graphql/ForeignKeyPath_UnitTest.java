package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import com.yg.inventory.model.db.ForeignKey;

import io.vavr.collection.List;

public class ForeignKeyPath_UnitTest {
    final String STEP_1 = "step_1";
    final String STEP_2 = "step_2";
    final String STEP_3 = "step_3";

    final List<String> PATH_1 = List.of(STEP_1, STEP_2);
    final List<String> PATH_2 = List.of(STEP_1, STEP_3);

    final ExtractValue EXTRACT_1 = mock(ExtractValue.class);
    final ExtractValue EXTRACT_2 = mock(ExtractValue.class);
    final ExtractValue EXTRACT_3 = mock(ExtractValue.class);

    final List<ExtractValue> EXTRACTS_1 = List.of(EXTRACT_1, EXTRACT_2);
    final List<ExtractValue> EXTRACTS_2 = List.of(EXTRACT_1, EXTRACT_3);

    final ForeignKey FOREIGN_KEY_2 = mock(ForeignKey.class);

    final ForeignKeyPath subject = new ForeignKeyPath(PATH_1, EXTRACTS_1);
    final ForeignKeyPath other1  = new ForeignKeyPath(PATH_1, EXTRACTS_1);
    final ForeignKeyPath other2  = new ForeignKeyPath(PATH_2, EXTRACTS_1);
    final ForeignKeyPath other3  = new ForeignKeyPath(PATH_1, EXTRACTS_2);

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
    }

    @Test
    void toString_test() {
        // Execute
        String result = subject.toString();

        // Verify
        assertTrue(result.contains(STEP_1));
        assertTrue(result.contains(STEP_2));
        assertTrue(result.contains(EXTRACT_1.toString()));
        assertTrue(result.contains(EXTRACT_2.toString()));
    }
}
