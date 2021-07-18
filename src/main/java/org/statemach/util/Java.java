package org.statemach.util;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.StackWalker.StackFrame;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.function.Function;

import graphql.com.google.common.base.Objects;
import io.vavr.collection.Stream;
import io.vavr.control.Option;

public interface Java {
    public static final Option<Boolean> OPTION_TRUE  = Option.of(Boolean.TRUE);
    public static final Option<Boolean> OPTION_FALSE = Option.of(Boolean.FALSE);

    static <E extends Exception> void soft(RunnableEx<E> runnable) {
        soft(runnable, ex -> new RuntimeException(ex));
    }

    static <E extends Exception, R extends RuntimeException> void soft(RunnableEx<E> runnable,
                                                                       Function<Throwable, R> toThrow) {
        try {
            runnable.run();
        } catch (Throwable ex) {
            throw toThrow.apply(ex);
        }
    }

    static <T, E extends Exception> T soft(SupplierEx<T, E> supplier) {
        return soft(supplier, ex -> new RuntimeException(ex));
    }

    static <T, E extends Exception, R extends RuntimeException> T soft(SupplierEx<T, E> supplier,
                                                                       Function<Throwable, R> toThrow) {
        try {
            return supplier.get();
        } catch (Throwable ex) {
            throw toThrow.apply(ex);
        }
    }

    static Class<?> getCallingClass(int depth) {
        return StackWalker.getInstance(RETAIN_CLASS_REFERENCE).walk(s -> s
            .skip(depth)
            .map(StackFrame::getDeclaringClass)
            .findFirst()
            .get());
    }

    static String resource(String resource) {
        return resource(getCallingClass(2), resource);
    }

    static String resource(Class<?> forClass, String resource) {
        try (Scanner scanner = new Scanner(forClass.getResourceAsStream(resource), StandardCharsets.UTF_8)) {
            return scanner.useDelimiter("\\A").next();
        }
    }

    static boolean isEmpty(String val) {
        return (null == val || 0 == val.length());
    }

    static String toStringOrEmpty(Object val) {
        return null == val ? "" : val.toString();
    }

    static String toString(Throwable ex) {
        try (StringWriter sw = new StringWriter()) {
            try (PrintWriter pw = new PrintWriter(sw)) {
                ex.printStackTrace(pw);
                pw.flush();
                return sw.toString();
            }
        } catch (Exception io) {
            throw new RuntimeException(io);
        }
    }

    static String format(String format, Object... params) {
        return Stream.range(0, params.length)
            .foldLeft(format, (f, i) -> f.replace("${" + i + "}", toStringOrEmpty(params[i])));
    }

    static String repeat(String repeat, int count) {
        return Stream.range(0, count).map(i -> repeat).mkString();
    }

    static String repeat(String repeat, String separator, int count) {
        return Stream.range(0, count).map(i -> repeat).mkString(separator);
    }

    static <T> T ifNull(T value, T inCaseValueIsNull) {
        return null != value ? value : inCaseValueIsNull;
    }

    static <R> Function<Object, R> asString(Function<String, R> f) {
        return o -> f.apply((String) o);
    }

    static <R> Function<Object, R> asNumber(Function<Number, R> f) {
        return o -> f.apply((Number) o);
    }

    static <R> Function<Object, R> asBoolean(Function<Boolean, R> f) {
        return o -> f.apply((Boolean) o);
    }

    @SafeVarargs
    static <T> boolean in(T value, T... checks) {
        for (T check : checks) {
            if (Objects.equal(value, check)) {
                return true;
            }
        }
        return false;
    }

    static Option<Boolean> equalsPreCheck(Object a, Object b) {
        if (a == b) {
            return OPTION_TRUE;
        }
        if (null == a || null == b) {
            return OPTION_FALSE;
        }
        if (a.getClass() != b.getClass()) {
            return OPTION_FALSE;
        }
        return Option.none();
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    static <T> Option<Boolean> equalsCheck(T self, Object b, Function<T, ?>... fields) {
        return equalsPreCheck(self, b).orElse(() -> {
            T other = (T) b;
            for (Function<T, ?> field : fields) {
                if (!Objects.equal(field.apply(self), field.apply(other))) {
                    return OPTION_FALSE;
                }
            }
            return Option.none();
        });
    }

    @SafeVarargs
    static <T> boolean equalsByFields(T self, Object b, Function<T, ?>... fields) {
        return equalsCheck(self, b, fields).getOrElse(true);
    }

    static <T extends Comparable<T>> int compareTo(T left, T right) {
        if (null == left && null == right) {
            return 0;
        }
        if (null == left) {
            return -1;
        }
        if (null == right) {
            return 1;
        }
        return left.compareTo(right);
    }

    @SafeVarargs
    static <T> int compareByFields(T left, T right, Function<T, ? extends Comparable<?>>... fields) {
        for (Function<T, ? extends Comparable<?>> field : fields) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            int cmp = compareTo((Comparable) field.apply(left), (Comparable) field.apply(right));
            if (0 != cmp) {
                return cmp;
            }
        }
        return 0;
    }

}
