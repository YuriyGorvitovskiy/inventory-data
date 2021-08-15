package org.statemach.db.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.statemach.db.jdbc.Vendor;
import org.statemach.db.schema.DataType;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.SQLBuilder;
import org.statemach.db.sql.postgres.PostgresDataType;
import org.statemach.db.sql.postgres.PostgresSQLBuilder;

import io.vavr.collection.List;
import io.vavr.collection.Seq;

public class Filter_UnitTest {
    final String STEP_1 = "step_1";
    final String STEP_2 = "step_2";
    final String STEP_3 = "step_3";

    final List<String> PATH_1 = List.of(STEP_1, STEP_2);
    final List<String> PATH_2 = List.of(STEP_1, STEP_3);

    final DataType TYPE_1 = mock(DataType.class);
    final DataType TYPE_2 = mock(DataType.class);

    final String VALUE_1 = "Value 1";
    final String VALUE_2 = "Value 2";
    final String VALUE_3 = "Value 3";

    final Seq<?> VALUES_1 = List.of(VALUE_1, VALUE_2);
    final Seq<?> VALUES_2 = List.of(VALUE_1, VALUE_3);

    final Filter subject = new Filter(PATH_1, true, TYPE_1, false, Filter.Operator.EQUAL, VALUES_1);
    final Filter other1  = new Filter(PATH_1, true, TYPE_1, false, Filter.Operator.EQUAL, VALUES_1);
    final Filter other2  = new Filter(PATH_2, true, TYPE_1, false, Filter.Operator.EQUAL, VALUES_1);
    final Filter other3  = new Filter(PATH_1, false, TYPE_1, false, Filter.Operator.EQUAL, VALUES_1);
    final Filter other4  = new Filter(PATH_1, true, TYPE_2, false, Filter.Operator.EQUAL, VALUES_1);
    final Filter other5  = new Filter(PATH_1, true, TYPE_1, true, Filter.Operator.EQUAL, VALUES_1);
    final Filter other6  = new Filter(PATH_1, true, TYPE_1, false, Filter.Operator.TEXT_SEARCH, VALUES_1);
    final Filter other7  = new Filter(PATH_1, true, TYPE_1, false, Filter.Operator.EQUAL, VALUES_2);

    final GraphQLMapping    mapping    = GraphQLMapping.of(Vendor.POSTGRES);
    final SQLBuilder        builder    = new PostgresSQLBuilder("test");
    final PreparedStatement statement  = mock(PreparedStatement.class);
    final Connection        connection = mock(Connection.class);
    final Array             array      = mock(Array.class);

    @BeforeEach
    void before() throws Exception {
        doReturn(connection).when(statement).getConnection();
        doReturn(array).when(connection).createArrayOf(any(), any());
    }

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
        assertNotEquals(other5, subject);
        assertNotEquals(other6, subject);
        assertNotEquals(other7, subject);
    }

    @Test
    void toString_test() {
        // Execute
        String result1 = subject.toString();
        String result2 = other3.toString();
        String result3 = other5.toString();

        // Verify
        assertTrue(result1.contains(Filter.Operator.EQUAL.toString()));
        assertTrue(result1.contains("*" + TYPE_1.toString()));
        assertTrue(result1.contains(VALUE_1));
        assertTrue(result1.contains(VALUE_2));
        assertFalse(result2.contains("accepts NULL"));

        assertFalse(result2.contains("*"));

        assertTrue(result3.contains("accepts NULL"));
    }

    @Test
    void buildCondition_equals() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.EQUAL,
                List.of(VALUE_1));

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("t.step_2 = ?", result.sql);
        assertEquals(2, result.inject.set(statement, 1));
        verify(statement).setString(1, VALUE_1);
    }

    @Test
    void buildCondition_equals_null() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                true,
                Filter.Operator.EQUAL,
                List.of(VALUE_1));

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("(t.step_2 IS NULL) OR (t.step_2 = ?)", result.sql);
        assertEquals(2, result.inject.set(statement, 1));
        verify(statement).setString(1, VALUE_1);
    }

    @Test
    void buildCondition_in_param() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.IN_PARAM,
                List.of(VALUE_1, VALUE_2));

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("t.step_2 IN (?, ?)", result.sql);
        assertEquals(3, result.inject.set(statement, 1));
        verify(statement).setString(1, VALUE_1);
        verify(statement).setString(2, VALUE_2);
    }

    @Test
    void buildCondition_in_param_null() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                true,
                Filter.Operator.IN_PARAM,
                List.of(VALUE_1, VALUE_2));

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("(t.step_2 IS NULL) OR (t.step_2 IN (?, ?))", result.sql);
        assertEquals(3, result.inject.set(statement, 1));
        verify(statement).setString(1, VALUE_1);
        verify(statement).setString(2, VALUE_2);
    }

    @Test
    void buildCondition_in_table() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.IN_TABLE,
                List.of(VALUE_1, VALUE_2));

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("t.step_2 IN (SELECT UNNEST((?)::character varying[]))", result.sql);
        assertEquals(2, result.inject.set(statement, 1));
        verify(statement).setArray(1, array);
    }

    @Test
    void buildCondition_in_table_null() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                true,
                Filter.Operator.IN_TABLE,
                List.of(VALUE_1, VALUE_2));

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("(t.step_2 IS NULL) OR (t.step_2 IN (SELECT UNNEST((?)::character varying[])))", result.sql);
        assertEquals(2, result.inject.set(statement, 1));
        verify(statement).setArray(1, array);
    }

    @Test
    void buildCondition_is_null() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                true,
                Filter.Operator.NULL_CHECK,
                List.empty());

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("t.step_2 IS NULL", result.sql);
        assertEquals(1, result.inject.set(statement, 1));
        verifyNoMoreInteractions(statement);
    }

    @Test
    void buildCondition_is_not_null() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.NULL_CHECK,
                List.empty());

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("t.step_2 IS NOT NULL", result.sql);
        assertEquals(1, result.inject.set(statement, 1));
        verifyNoMoreInteractions(statement);
    }

    @Test
    void buildCondition_text_search() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.TEXT_SEARCH,
                List.of(VALUE_1, VALUE_2));

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertEquals("websearch_to_tsquery('english', ?) @@ t.step_2", result.sql);
        assertEquals(2, result.inject.set(statement, 1));
        verify(statement).setString(1, VALUE_1 + " " + VALUE_2);

    }

    @Test
    void buildCondition_null() throws Exception {
        // Setup
        final Filter subject = new Filter(PATH_1,
                true,
                PostgresDataType.CHARACTER_VARYING,
                false,
                null,
                List.of(VALUE_1, VALUE_2));

        // Execute
        Condition result = subject.buildCondition(mapping, builder, "t");

        // Verify
        assertNull(result);
    }

    @Test
    void of_text_search() {
        // Execute
        Filter result = Filter.of(PATH_1, false, PostgresDataType.TSVECTOR, Arrays.asList(VALUE_1, VALUE_2));

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.TSVECTOR,
                false,
                Filter.Operator.TEXT_SEARCH,
                VALUES_1), result);
    }

    @Test
    void of_not_null() {
        // Execute
        Filter result = Filter.of(PATH_1, false, PostgresDataType.CHARACTER_VARYING, Collections.emptyList());

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.NULL_CHECK,
                List.empty()), result);
    }

    @Test
    void of_null() {
        // Execute
        Filter result = Filter.of(PATH_1, false, PostgresDataType.CHARACTER_VARYING, Arrays.asList((String) null));

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                true,
                Filter.Operator.NULL_CHECK,
                List.empty()), result);
    }

    @Test
    void of_equal() {
        // Execute
        Filter result = Filter.of(PATH_1, false, PostgresDataType.CHARACTER_VARYING, Arrays.asList(VALUE_1));

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.EQUAL,
                List.of(VALUE_1)), result);
    }

    @Test
    void of_equal_and_null() {
        // Execute
        Filter result = Filter.of(PATH_1, false, PostgresDataType.CHARACTER_VARYING, Arrays.asList(VALUE_1, null));

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                true,
                Filter.Operator.EQUAL,
                List.of(VALUE_1)), result);
    }

    @Test
    void of_in_param() {
        // Execute
        Filter result = Filter.of(PATH_1, false, PostgresDataType.CHARACTER_VARYING, Arrays.asList(VALUE_1, VALUE_2));

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.IN_PARAM,
                List.of(VALUE_1, VALUE_2)), result);
    }

    @Test
    void of_in_param_or_null() {
        // Execute
        Filter result = Filter.of(PATH_1, false, PostgresDataType.CHARACTER_VARYING, Arrays.asList(VALUE_1, null, VALUE_2));

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                true,
                Filter.Operator.IN_PARAM,
                List.of(VALUE_1, VALUE_2)), result);
    }

    @Test
    void of_in_table() {
        // Execute
        Filter result = Filter.of(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                Arrays.asList(VALUE_1,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2));

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                false,
                Filter.Operator.IN_TABLE,
                List.of(VALUE_1,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2)),
                result);
    }

    @Test
    void of_in_table_or_null() {
        // Execute
        Filter result = Filter.of(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                Arrays.asList(null,
                        VALUE_1,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2));

        // Verify
        assertEquals(new Filter(PATH_1,
                false,
                PostgresDataType.CHARACTER_VARYING,
                true,
                Filter.Operator.IN_TABLE,
                List.of(VALUE_1,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2,
                        VALUE_2)),
                result);
    }
}
