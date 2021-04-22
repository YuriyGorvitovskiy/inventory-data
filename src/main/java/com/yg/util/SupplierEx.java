package com.yg.util;

@FunctionalInterface
public interface SupplierEx<T, E extends Exception> {
    T get() throws E;
}
