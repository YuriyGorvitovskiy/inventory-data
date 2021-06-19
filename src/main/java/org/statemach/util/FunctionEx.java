package org.statemach.util;

@FunctionalInterface
public interface FunctionEx<P, R, E extends Exception> {
    R apply(P param) throws E;
}
