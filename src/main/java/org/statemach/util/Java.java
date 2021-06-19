package org.statemach.util;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.StackWalker.StackFrame;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import java.util.function.Function;

import graphql.com.google.common.base.Objects;
import io.vavr.collection.Stream;

public interface Java {
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

    static String toString(Object val) {
        return null == val ? "" : val.toString();
    }

    static String toString(Throwable ex) {
        try (StringWriter sw = new StringWriter()) {
            try (PrintWriter pw = new PrintWriter(sw)) {
                ex.printStackTrace(pw);
                pw.flush();
                return sw.toString();
            }
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

    static String format(String format, Object... params) {
        return Stream.range(0, params.length)
            .foldLeft(format, (f, i) -> f.replace("${" + i + "}", toString(params[i])));
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

    @SafeVarargs
    static <T> boolean in(T value, T... checks) {
        for (T check : checks) {
            if (Objects.equal(value, check)) {
                return true;
            }
        }
        return false;
    }

    static <T> boolean in(T value, List<T> checks) {
        return checks.contains(value);
    }

}
