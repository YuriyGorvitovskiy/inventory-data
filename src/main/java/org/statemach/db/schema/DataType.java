package org.statemach.db.schema;

import java.util.Objects;
import java.util.function.Function;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Injector;
import org.statemach.db.jdbc.Setter;
import org.statemach.util.Java;

import io.vavr.collection.Traversable;

public class DataType {

    public final String name;

    public final Injector<String>                        injectStringValue;
    public final Injector<Traversable<? extends String>> injectStringArray;
    public final Injector<Object>                        injectJsonValue;
    public final Injector<Traversable<?>>                injectJsonArray;
    public final Extract<?>                              extractJsonValue;

    public final boolean isMutable;
    public final boolean isExtractable;
    public final boolean isFilterable;

    public DataType(String name,
                    Injector<String> injectStringValue,
                    Injector<Traversable<? extends String>> injectStringArray,
                    Injector<Object> injectJsonValue,
                    Injector<Traversable<?>> injectJsonArray,
                    Extract<?> extractJsonValue,
                    boolean isMutable,
                    boolean isExtractable,
                    boolean isFilterable) {
        this.name = name;
        this.injectStringValue = injectStringValue;
        this.injectStringArray = injectStringArray;
        this.injectJsonValue = injectJsonValue;
        this.injectJsonArray = injectJsonArray;
        this.extractJsonValue = extractJsonValue;
        this.isMutable = isMutable;
        this.isExtractable = isExtractable;
        this.isFilterable = isFilterable;
    }

    public static DataType unsupported(String name) {
        return new DataType(name, null, null, null, null, null, false, false, false);
    }

    public static <K> DataType of(String name,
                                  int dbType,
                                  Function<String, K> str,
                                  Function<Object, K> jsn,
                                  Setter<? super K> setter,
                                  Extract<?> extract) {
        return new DataType(name,
                Injector.of(str, dbType, setter),
                Injector.ofArray(str, name),
                Injector.of(jsn, dbType, setter),
                Injector.ofArray(jsn, name),
                extract,
                null != extract,
                null != extract,
                true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name);
    }

    @Override
    public String toString() {
        return "DataType@{name: '" + name + "'}";
    }
}
