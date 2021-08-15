package org.statemach.util;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.jackson.datatype.VavrModule;

public interface Json {
    static final DateTimeFormatter ISO8601_FORMAT = DateTimeFormatter.ISO_INSTANT;

    static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(new VavrModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(SerializationFeature.INDENT_OUTPUT, true);

    static <T> T parse(String input, Class<T> type) {
        return Java.soft(() -> {
            return Json.MAPPER.readValue(input, type);
        });
    }

    static String toISO8601(Instant v) {
        return null == v ? null : ISO8601_FORMAT.format(v);
    }

    static Instant fromISO8601(String v) {
        return null == v ? null : ISO8601_FORMAT.parse(v, Instant::from);
    }

    @SuppressWarnings("unchecked")
    static Object alphabetize(Object input) {
        if (input instanceof Map) {
            return ((Map<String, Object>) input).toList()
                .sortBy(t -> t._1)
                .toLinkedMap(t -> t)
                .mapValues(Json::alphabetize);
        }
        if (input instanceof java.util.Map) {
            return HashMap.ofAll((java.util.Map<String, Object>) input)
                .toList()
                .sortBy(t -> t._1).toLinkedMap(t -> t)
                .toLinkedMap(t -> t)
                .mapValues(Json::alphabetize);
        }
        if (input instanceof Iterable) {
            return List.ofAll((Iterable<Object>) input)
                .map(Json::alphabetize);
        }
        return input;
    }

    static Object readAlphabetize(String json) {
        return Java.soft(() -> alphabetize(MAPPER.readValue(json, Object.class)));
    }
}
