package com.yg.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.vavr.jackson.datatype.VavrModule;

public interface Json {
    public final static ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(new VavrModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(SerializationFeature.INDENT_OUTPUT, true);

    static <T> T parse(String input, Class<T> type) {
        return Java.soft(() -> {
            return Json.MAPPER.readValue(input, type);
        });
    }

}
