package org.statemach.db.schema;

import java.util.function.Function;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;

public class DataType<T> {

    public final String              name;
    public final Class<T>            java;
    public final Extract<T>          extract;
    public final Function<T, Inject> injector;

    public DataType(String name, Class<T> java, Extract<T> extract, Function<T, Inject> injector) {
        this.name = name;
        this.java = java;
        this.extract = extract;
        this.injector = injector;
    }

    public Inject inject(T value) {
        return injector.apply(value);
    }
}
