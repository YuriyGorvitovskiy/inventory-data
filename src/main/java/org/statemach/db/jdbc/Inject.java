package org.statemach.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.statemach.util.Java;

import io.vavr.collection.Traversable;

@FunctionalInterface
public interface Inject {

    int set(PreparedStatement ps, int pos) throws SQLException;

    static final Inject NOTHING = (ps, i) -> i;

    /// Return next position
    @SafeVarargs
    static int inject(PreparedStatement ps, int pos, Traversable<Inject>... portions) {
        for (Traversable<Inject> portion : portions) {
            pos = portion.foldLeft(pos, (i, j) -> Java.soft(() -> j.set(ps, i)));
        }
        return pos;
    }

    static Inject fold(Traversable<Inject> injects) {
        return injects.foldLeft(Inject.NOTHING, (f, j) -> (ps, i) -> j.set(ps, f.set(ps, i)));
    }

    @SuppressWarnings("unchecked")
    static <T> Inject of(Injector<T> injector, Object value) {
        return injector.prepare((T) value);
    }

}
