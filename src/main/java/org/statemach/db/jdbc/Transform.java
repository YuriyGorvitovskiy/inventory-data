package org.statemach.db.jdbc;

import java.sql.Timestamp;
import java.util.UUID;
import java.util.function.Function;

import org.statemach.util.Json;

public interface Transform {

    static final Function<Object, Boolean>   BOOLEAN              = b -> (Boolean) b;
    static final Function<Object, Long>      NUMBER_TO_LONG       = n -> ((Number) n).longValue();
    static final Function<Object, Integer>   NUMBER_TO_INTEGER    = n -> ((Number) n).intValue();
    static final Function<Object, Double>    NUMBER_TO_DOUBLE     = n -> ((Number) n).doubleValue();
    static final Function<Object, String>    STRING               = s -> (String) s;
    static final Function<Object, Boolean>   STRING_TO_BOOLEAN    = s -> Boolean.parseBoolean((String) s);
    static final Function<Object, Long>      STRING_TO_LONG       = s -> Long.parseLong((String) s);
    static final Function<Object, Integer>   STRING_TO_INTEGER    = s -> Integer.parseInt((String) s);
    static final Function<Object, Double>    STRING_TO_DOUBLE     = s -> Double.parseDouble((String) s);
    static final Function<Object, UUID>      STRING_TO_UUID       = s -> UUID.fromString((String) s);
    static final Function<Object, Timestamp> ISO8601_TO_TIMESTAMP = s -> new Timestamp(
            Json.fromISO8601((String) s).toEpochMilli());

}
