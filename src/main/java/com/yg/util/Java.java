package com.yg.util;

public interface Java {
    static <E extends Exception> void soft(RunnableEx<E> runnable) {
        try {
            runnable.run();
        } catch (Throwable ex) {
            throw new RuntimeException(ex);
        }
    }

    static <T, E extends Exception> T soft(SupplierEx<T, E> supplier) {
        try {
            return supplier.get();
        } catch (Throwable ex) {
            throw new RuntimeException(ex);
        }
    }
}
