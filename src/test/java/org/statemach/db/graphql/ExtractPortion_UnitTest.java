package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import io.vavr.collection.List;

public class ExtractPortion_UnitTest {
    final ForeignKeyPath KEY_1 = mock(ForeignKeyPath.class);
    final ForeignKeyPath KEY_2 = mock(ForeignKeyPath.class);
    final ForeignKeyPath KEY_3 = mock(ForeignKeyPath.class);

    final ExtractValue VALUE_1 = mock(ExtractValue.class);
    final ExtractValue VALUE_2 = mock(ExtractValue.class);
    final ExtractValue VALUE_3 = mock(ExtractValue.class);

    final SubQuery QUERY_1 = mock(SubQuery.class);
    final SubQuery QUERY_2 = mock(SubQuery.class);
    final SubQuery QUERY_3 = mock(SubQuery.class);

    final List<ForeignKeyPath> KEYS_1 = List.of(KEY_1, KEY_2);
    final List<ForeignKeyPath> KEYS_2 = List.of(KEY_1, KEY_3);

    final List<ExtractValue> VALUES_1 = List.of(VALUE_1, VALUE_2);
    final List<ExtractValue> VALUES_2 = List.of(VALUE_1, VALUE_3);

    final List<SubQuery> QUERIES_1 = List.of(QUERY_1, QUERY_2);
    final List<SubQuery> QUERIES_2 = List.of(QUERY_1, QUERY_3);

    final ExtractPortion subject = new ExtractPortion(KEYS_1, VALUES_1, QUERIES_1);
    final ExtractPortion other1  = new ExtractPortion(KEYS_1, VALUES_1, QUERIES_1);
    final ExtractPortion other2  = new ExtractPortion(KEYS_2, VALUES_1, QUERIES_1);
    final ExtractPortion other3  = new ExtractPortion(KEYS_1, VALUES_2, QUERIES_1);
    final ExtractPortion other4  = new ExtractPortion(KEYS_1, VALUES_1, QUERIES_2);

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
        assertTrue(result.contains(KEY_1.toString()));
        assertTrue(result.contains(KEY_2.toString()));
        assertTrue(result.contains(VALUE_1.toString()));
        assertTrue(result.contains(VALUE_2.toString()));
        assertTrue(result.contains(QUERY_1.toString()));
        assertTrue(result.contains(QUERY_2.toString()));
    }
}
