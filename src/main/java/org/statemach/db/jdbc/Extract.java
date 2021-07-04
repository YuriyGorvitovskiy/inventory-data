package org.statemach.db.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Iterator;

import org.statemach.util.Java;
import org.statemach.util.Json;

import io.vavr.Tuple2;
import io.vavr.collection.Map;
import io.vavr.collection.Traversable;

@FunctionalInterface
public interface Extract<T> {

    static final Extract<Boolean>        BOOLEAN               = (rs, i) -> {
                                                                   boolean v = rs.getBoolean(i);
                                                                   return rs.wasNull() ? null : v;
                                                               };
    static final Extract<Double>         DOUBLE                = (rs, i) -> {
                                                                   double v = rs.getDouble(i);
                                                                   return rs.wasNull() ? null : v;
                                                               };
    static final Extract<Integer>        INTEGER               = (rs, i) -> {
                                                                   int v = rs.getInt(i);
                                                                   return rs.wasNull() ? null : v;
                                                               };
    static final Extract<Long>           LONG                  = (rs, i) -> {
                                                                   long v = rs.getLong(i);
                                                                   return rs.wasNull() ? null : v;
                                                               };
    static final Extract<String>         LONG_AS_STRING        = (rs, i) -> {
                                                                   long v = rs.getLong(i);
                                                                   return rs.wasNull() ? null : Long.toString(v);
                                                               };
    static final Extract<java.util.UUID> OBJECT_AS_UUID        = (rs, i) -> {
                                                                   java.util.UUID value = (java.util.UUID) rs.getObject(i);
                                                                   return rs.wasNull() ? null : value;
                                                               };
    static final Extract<String>         OBJECT_AS_UUID_STRING = (rs, i) -> {
                                                                   java.util.UUID value = (java.util.UUID) rs.getObject(i);
                                                                   return rs.wasNull() ? null : value.toString();
                                                               };
    static final Extract<String>         STRING                = (rs, i) -> rs.getString(i);
    static final Extract<Timestamp>      TIMESTAMP             = (rs, i) -> rs.getTimestamp(i);
    static final Extract<Instant>        TIMESTAMP_AS_INSTANT  = (rs, i) -> {
                                                                   Timestamp v = rs.getTimestamp(i);
                                                                   return null == v ? null : Instant.ofEpochMilli(v.getTime());
                                                               };
    static final Extract<String>         TIMESTAMP_AS_ISO8601  = (rs, i) -> Json.toISO8601(TIMESTAMP_AS_INSTANT.get(rs, i));
    static final Extract<Void>           VOID                  = (rs, pos) -> null;

    T get(ResultSet rs, int pos) throws SQLException;

    default int next(int pos) {
        return pos + 1;
    }

    static Map<String, Object> extract(ResultSet rs,
                                       int pos,
                                       Traversable<Tuple2<String, Extract<?>>> extracts) {
        // We should force toMap() call at the end, to collect all records
        // Otherwise this stream will be lazy, and
        // ResultSet.next() will be called before all fields read
        return extracts.zipWith(positions(pos, extracts.map(e -> e._2)),
                (t, p) -> Java.soft(() -> new Tuple2<String, Object>(t._1, t._2.get(rs, p))))
            .toMap(t -> t);
    }

    static Iterable<Integer> positions(int start, Iterable<Extract<?>> extracts) {
        return () -> new PositionIterator(start, extracts.iterator());
    }

    static class PositionIterator implements Iterator<Integer> {
        int                  pos;
        Iterator<Extract<?>> it;

        PositionIterator(int pos, Iterator<Extract<?>> it) {
            this.pos = pos;
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Integer next() {
            int i = pos;
            pos = it.next().next(pos);
            return i;
        }
    }

}