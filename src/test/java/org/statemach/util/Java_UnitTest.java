package org.statemach.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;

import org.junit.jupiter.api.Test;

import io.vavr.control.Option;

public class Java_UnitTest {

    @Test
    void soft_runnable() {
        // Execute
        assertDoesNotThrow(() -> Java.soft(() -> {}));
    }

    @Test
    void soft_runnable_error() {
        // Setup
        Exception ex = new Exception();

        // Execute
        @SuppressWarnings({ "unchecked", "rawtypes" })
        RuntimeException re = assertThrows(RuntimeException.class,
                () -> Java.soft((RunnableEx) () -> {
                    throw ex;
                }));

        // Verify
        assertSame(ex, re.getCause());
    }

    @Test
    void soft_supplier() {
        // Execute
        assertEquals(1, Java.soft(() -> 1));
    }

    @Test
    void soft_supplier_error() {
        // Setup
        Exception ex = new Exception();

        // Execute
        @SuppressWarnings({ "unchecked", "rawtypes" })
        RuntimeException re = assertThrows(RuntimeException.class,
                () -> Java.soft((SupplierEx) () -> {
                    throw ex;
                }));

        // Verify
        assertSame(ex, re.getCause());
    }

    @Test
    void isEmpty() {
        // Execute & Validation
        assertTrue(Java.isEmpty(null));
        assertTrue(Java.isEmpty(""));
        assertFalse(Java.isEmpty(" "));
        assertFalse(Java.isEmpty("Hello"));
    }

    @Test
    void toStringOrEmpty() {
        // Execute & Validation
        assertEquals("", Java.toStringOrEmpty(null));
        assertEquals("", Java.toStringOrEmpty(""));
        assertEquals(" ", Java.toStringOrEmpty(" "));
        assertEquals("Hello", Java.toStringOrEmpty("Hello"));
        assertEquals("1", Java.toStringOrEmpty(1));
    }

    @Test
    void toString_exception() {
        // Setup
        Exception ex = new Exception("Test error");

        // Execute
        String result = Java.toString(ex);

        // Verify
        assertTrue(result.contains("Test error"));
        assertTrue(result.contains(Exception.class.getName()));
        assertTrue(result.contains(getClass().getName() + ".toString_exception"));
    }

    @Test
    void toString_exception_failure() {
        // Setup
        @SuppressWarnings("serial")
        Exception ex = new Exception() {
            @Override
            public void printStackTrace(PrintWriter pw) {
                throw new RuntimeException();
            }
        };

        // Execute
        assertThrows(RuntimeException.class, () -> Java.toString(ex));
    }

    @Test
    void format() {
        // Execute
        String result = Java.format("String: ${1}, Integer: ${0}", 34, "Hello");

        // Verify
        assertEquals("String: Hello, Integer: 34", result);
    }

    @Test
    void repeat() {
        // Execute
        String result = Java.repeat("Ab", 3);

        // Verify
        assertEquals("AbAbAb", result);
    }

    @Test
    void repeat_separator() {
        // Execute
        String result = Java.repeat("Ab", ", ", 4);

        // Verify
        assertEquals("Ab, Ab, Ab, Ab", result);
    }

    @Test
    void ifNull() {
        // Execute & Verify
        assertEquals(34, Java.ifNull(null, 34));
        assertEquals(12, Java.ifNull(12, 34));
    }

    @Test
    void in() {
        // Execute & Verify
        assertFalse(Java.in(12));
        assertFalse(Java.in(12, 13, 14));
        assertTrue(Java.in(12, 13, 12, 14));
    }

    @Test
    void equalsPreCheck() {
        // Setup
        Object a = new Object();
        Object b = new Object();

        // Execute & Verify
        assertSame(Java.OPTION_TRUE, Java.equalsPreCheck(a, a));
        assertSame(Java.OPTION_FALSE, Java.equalsPreCheck(a, null));
        assertSame(Java.OPTION_FALSE, Java.equalsPreCheck(null, b));
        assertSame(Java.OPTION_FALSE, Java.equalsPreCheck(this, b));
        assertSame(Option.none(), Java.equalsPreCheck(a, b));
    }

    @Test
    void equalsCheck() {
        // Setup
        Object a = new Object();
        Object b = new Object();

        // Execute & Verify
        assertSame(Java.OPTION_TRUE, Java.equalsCheck(a, a));
        assertSame(Java.OPTION_FALSE, Java.equalsCheck(a, null));
        assertSame(Java.OPTION_FALSE, Java.equalsCheck(null, b));
        assertSame(Java.OPTION_FALSE, Java.equalsCheck(this, b));

        assertSame(Java.OPTION_FALSE, Java.equalsCheck(a, b, (o) -> 1, (o) -> o == a));

        assertSame(Option.none(), Java.equalsCheck(a, b, (o) -> 1, (o) -> null));
    }

    @Test
    void equalsByFields() {
        // Setup
        Object a = new Object();
        Object b = new Object();

        // Execute & Verify
        assertTrue(Java.equalsByFields(a, a));
        assertFalse(Java.equalsByFields(a, null));
        assertFalse(Java.equalsByFields(null, b));
        assertFalse(Java.equalsByFields(this, b));

        assertFalse(Java.equalsByFields(a, b, (o) -> 1, (o) -> o == a));

        assertTrue(Java.equalsByFields(a, b, (o) -> 1, (o) -> null));
    }

    @Test
    void compareTo() {
        // Setup
        String a = "Hello";
        String b = "World";

        // Execute & Verify
        assertEquals(0, Java.compareTo(null, null));
        assertEquals(1, Java.compareTo(a, null));
        assertEquals(-1, Java.compareTo(null, b));
        assertTrue(0 > Java.compareTo(a, b));
    }

    @Test
    void compareByFields() {
        // Setup
        Object a = new Object();
        Object b = new Object();

        // Execute & Verify
        assertEquals(0, Java.compareByFields(a, b));
        assertEquals(0, Java.compareByFields(a, b, (o) -> "Hi", (o) -> "Hello"));
        assertTrue(0 > Java.compareByFields(a, b, (o) -> "Hi", (o) -> o == a ? "Hello" : "World"));
        assertTrue(0 < Java.compareByFields(b, a, (o) -> "Hi", (o) -> o == a ? "Hello" : "World"));
    }

}
