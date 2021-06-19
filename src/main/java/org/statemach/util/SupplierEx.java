package org.statemach.util;

@FunctionalInterface
public interface SupplierEx<R, E extends Exception> {
    R get() throws E;
}
