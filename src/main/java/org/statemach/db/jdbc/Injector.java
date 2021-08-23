package org.statemach.db.jdbc;

import java.sql.Types;
import java.util.function.Function;

import io.vavr.collection.Traversable;

@FunctionalInterface
public interface Injector<T> {

    Inject prepare(T value);

    static <T, K> Injector<T> of(Function<T, K> mapping, int sqlType, Setter<? super K> setter) {
        return (v) -> (ps, i) -> {
            if (null == v) {
                ps.setNull(i, sqlType);
            } else {
                setter.set(ps, i, mapping.apply(v));
            }
            return i + 1;
        };
    }

    static <T, K> Injector<Traversable<? extends T>> ofArray(Function<T, K> mapping, String dbType) {
        return (v) -> (ps, i) -> {
            if (null == v) {
                ps.setNull(i, Types.ARRAY);
            } else {
                ps.setArray(i,
                        ps.getConnection().createArrayOf(
                                dbType,
                                v.map(e -> null == e ? null : mapping.apply(e)).toJavaArray()));
            }
            return i + 1;
        };
    }

    static final Injector<Boolean>        BOOLEAN = Injector.of(t -> t, Types.BOOLEAN, Setter.BOOLEAN);
    static final Injector<Double>         DOUBLE  = Injector.of(t -> t, Types.DOUBLE, Setter.DOUBLE);
    static final Injector<Integer>        INTEGER = Injector.of(t -> t, Types.INTEGER, Setter.INTEGER);
    static final Injector<Long>           LONG    = Injector.of(t -> t, Types.BIGINT, Setter.LONG);
    static final Injector<String>         STRING  = Injector.of(t -> t, Types.VARCHAR, Setter.STRING);
    static final Injector<java.util.UUID> UUID    = Injector.of(t -> t, Types.OTHER, Setter.OBJECT);
    static final Injector<Object>         INSTANT = Injector.of(Transform.Jsn.TIMESTAMP, Types.TIMESTAMP, Setter.TIMESTAMP);

}