package org.statemach.util;

@FunctionalInterface
public interface ConsumerEx<P, E extends Exception> {
    void accept(P param) throws E;
}
