package org.statemach.util;

@FunctionalInterface
public interface RunnableEx<E extends Exception> {
    void run() throws E;
}
