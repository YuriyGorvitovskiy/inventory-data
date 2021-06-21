package org.statemach.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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

    static DateFormat newISO8601Format() {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        return format;
    }

    static String toISO8601(Instant v) {
        return null == v ? null : ISO8601_FORMAT.format(v);
    }

    static Instant fromISO8601(String v) {
        return null == v ? null : ISO8601_FORMAT.parse(v, Instant::from);
    }
}
