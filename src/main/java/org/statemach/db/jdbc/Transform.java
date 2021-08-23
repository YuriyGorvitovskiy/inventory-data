package org.statemach.db.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;

import org.statemach.util.Json;

public interface Transform {

    public static interface Jsn {
        static final Function<Object, Boolean>   BOOLEAN   = v -> (Boolean) v;
        static final Function<Object, Double>    DOUBLE    = v -> ((Number) v).doubleValue();
        static final Function<Object, Integer>   INTEGER   = v -> ((Number) v).intValue();
        static final Function<Object, Long>      LONG      = v -> v instanceof Number
                ? ((Number) v).longValue()
                : Long.parseLong((String) v);
        static final Function<Object, String>    STRING    = v -> (String) v;
        static final Function<Object, Timestamp> TIMESTAMP = v -> v instanceof Instant
                ? new Timestamp(((Instant) v).toEpochMilli())
                : Str.TIMESTAMP.apply((String) v);
        static final Function<Object, UUID>      UUID      = v -> v instanceof java.util.UUID
                ? (java.util.UUID) v
                : Str.UUID.apply((String) v);
    }

    public static interface Str {
        static final Function<String, Boolean>   BOOLEAN   = Boolean::parseBoolean;
        static final Function<String, Double>    DOUBLE    = Double::parseDouble;
        static final Function<String, Integer>   INTEGER   = Integer::parseInt;
        static final Function<String, Long>      LONG      = Long::parseLong;
        static final Function<String, String>    STRING    = v -> v;
        static final Function<String, Timestamp> TIMESTAMP = v -> new Timestamp(Json.fromISO8601(v).toEpochMilli());
        static final Function<String, UUID>      UUID      = v -> java.util.UUID.fromString(v);
    }

}
