package org.statemach.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.statemach.util.Json;

import com.yg.util.DB.DataType;

import io.vavr.collection.Seq;

@FunctionalInterface
public interface Inject {
    static final BiFunction<DataType, Seq<?>, Inject> ARRAY                 = (t, v) -> (ps, i) -> {
                                                                                if (null == v)
                                                                                    ps.setNull(i, Types.ARRAY);
                                                                                else
                                                                                    ps.setArray(i,
                                                                                            ps.getConnection().createArrayOf(
                                                                                                    DataType.TO_DB_NAME.get(t)
                                                                                                        .get(),
                                                                                                    v.toJavaArray()));
                                                                                return i + 1;
                                                                            };
    static final Function<Boolean, Inject>            BOOLEAN               = (v) -> (ps, i) -> {
                                                                                if (null == v)
                                                                                    ps.setNull(i, Types.BOOLEAN);
                                                                                else
                                                                                    ps.setBoolean(i, v);
                                                                                return i + 1;
                                                                            };
    static final Function<Number, Inject>             DOUBLE                = (v) -> (ps, i) -> {
                                                                                if (null == v)
                                                                                    ps.setNull(i, Types.DOUBLE);
                                                                                else
                                                                                    ps.setDouble(i, v.doubleValue());
                                                                                return i + 1;
                                                                            };
    static final Function<Number, Inject>             INTEGER               = (v) -> (ps, i) -> {
                                                                                if (null == v)
                                                                                    ps.setNull(i, Types.INTEGER);
                                                                                else
                                                                                    ps.setInt(i, v.intValue());
                                                                                return i + 1;
                                                                            };
    static final Function<Number, Inject>             LONG                  = (v) -> (ps, i) -> {
                                                                                if (null == v)
                                                                                    ps.setNull(i, Types.BIGINT);
                                                                                else
                                                                                    ps.setLong(i, v.longValue());
                                                                                return i + 1;
                                                                            };
    static final Function<String, Inject>             STRING                = (v) -> (ps, i) -> {
                                                                                if (null == v)
                                                                                    ps.setNull(i, Types.VARCHAR);
                                                                                else
                                                                                    ps.setString(i, v);
                                                                                return i + 1;
                                                                            };
    static final Function<Timestamp, Inject>          TIMESTAMP             = (v) -> (ps, i) -> {
                                                                                if (null == v)
                                                                                    ps.setNull(i, Types.TIMESTAMP);
                                                                                else
                                                                                    ps.setTimestamp(i, v);
                                                                                return i + 1;
                                                                            };
    static final Function<UUID, Inject>               UUID_AS_OBJECT        = (v) -> (ps, i) -> {
                                                                                if (null == v) {
                                                                                    ps.setObject(i, new UUID(0L, 0L));
                                                                                } else {
                                                                                    ps.setObject(i, v);
                                                                                }
                                                                                return i + 1;
                                                                            };
    static final Function<Instant, Inject>            INSTANT_AS_TIMESTAMP  = (v) -> TIMESTAMP
        .apply(null == v ? null : new Timestamp(v.toEpochMilli()));
    static final Function<String, Inject>             ISO8601_AS_TIMESTAMP  = (v) -> INSTANT_AS_TIMESTAMP
        .apply(Json.fromISO8601(v));
    static final Function<String, Inject>             STRING_AS_BOOLEAN     = (v) -> BOOLEAN
        .apply(null == v ? null : Boolean.parseBoolean(v));
    static final Function<String, Inject>             STRING_AS_DOUBLE      = (v) -> DOUBLE
        .apply(null == v ? null : Double.parseDouble(v));
    static final Function<String, Inject>             STRING_AS_INTEGER     = (v) -> INTEGER
        .apply(null == v ? null : Integer.parseInt(v));
    static final Function<String, Inject>             STRING_AS_LONG        = (v) -> LONG
        .apply(null == v ? null : Long.parseLong(v));
    static final Function<String, Inject>             STRING_AS_UUID_OBJECT = (v) -> UUID_AS_OBJECT.apply(UUID.fromString(v));
    static final Function<String, Inject>             VOID                  = (v) -> (ps, i) -> i;

    /// return next position
    int set(PreparedStatement ps, int pos) throws SQLException;

}
