package org.statemach.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.vavr.Tuple2;
import io.vavr.collection.LinkedHashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class Json_UnitTest {
    @Test
    @SuppressWarnings("unchecked")
    void parse() {
        // Setup
        String json = Java.resource("simple.json");

        // Execute
        Map<String, Object> result = Json.parse(json, LinkedHashMap.class);

        // Verify
        assertEquals(
                LinkedHashMap.ofEntries(
                        new Tuple2<>("boolean", Boolean.TRUE),
                        new Tuple2<>("null", null),
                        new Tuple2<>("number", 4.23),
                        new Tuple2<>("string", "Hello")),
                result);
    }

    @Test
    void toISO8601() {
        // Execute
        String result = Json.toISO8601(Instant.ofEpochMilli(1625971578123L));

        // Verify
        assertEquals("2021-07-11T02:46:18.123Z", result);
    }

    @Test
    void toISO8601_null() {
        // Execute
        String result = Json.toISO8601(null);

        // Verify
        assertNull(result);
    }

    @Test
    void fromISO8601() {
        // Execute
        Instant result = Json.fromISO8601("2021-07-11T02:46:18.123Z");

        // Verify
        assertEquals(Instant.ofEpochMilli(1625971578123L), result);
    }

    @Test
    void fromISO8601_null() {
        // Execute
        Instant result = Json.fromISO8601(null);

        // Verify
        assertNull(result);
    }

    @Test
    void readAlphabetize() {
        // Setup
        String json = Java.resource("deep.json");

        // Execute
        Object result = Json.readAlphabetize(json);

        // Verify
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, Object> expect = LinkedHashMap.ofEntries(
                new Tuple2<>("boolean", true),
                new Tuple2<>("string", "Hello"),
                new Tuple2<>("number", 4.23),
                new Tuple2<>("null", null),
                new Tuple2<>("children",
                        List.of(
                                LinkedHashMap.ofEntries(
                                        new Tuple2<>("boolean", false),
                                        new Tuple2<>("string", "First"),
                                        new Tuple2<>("number", 1.23)),
                                LinkedHashMap.ofEntries(
                                        new Tuple2<>("boolean", true),
                                        new Tuple2<>("string", "Second"),
                                        new Tuple2<>("number", 2.34)),
                                LinkedHashMap.ofEntries(
                                        new Tuple2<>("boolean", false),
                                        new Tuple2<>("string", "Third"),
                                        new Tuple2<>("number", 3.45)))));

        assertEquals(expect, result);
    }

    @Test
    void alphabetize() {
        // Setup
        String json   = Java.resource("deep.json");
        Object object = Java.soft(() -> Json.MAPPER.readValue(json, LinkedHashMap.class));

        // Execute
        Object result = Json.alphabetize(object);

        // Verify
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, Object> expect = LinkedHashMap.ofEntries(
                new Tuple2<>("boolean", true),
                new Tuple2<>("string", "Hello"),
                new Tuple2<>("number", 4.23),
                new Tuple2<>("null", null),
                new Tuple2<>("children",
                        List.of(
                                LinkedHashMap.ofEntries(
                                        new Tuple2<>("boolean", false),
                                        new Tuple2<>("string", "First"),
                                        new Tuple2<>("number", 1.23)),
                                LinkedHashMap.ofEntries(
                                        new Tuple2<>("boolean", true),
                                        new Tuple2<>("string", "Second"),
                                        new Tuple2<>("number", 2.34)),
                                LinkedHashMap.ofEntries(
                                        new Tuple2<>("boolean", false),
                                        new Tuple2<>("string", "Third"),
                                        new Tuple2<>("number", 3.45)))));

        assertEquals(expect, result);
    }
}
