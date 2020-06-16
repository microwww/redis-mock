package com.github.microwww.util;

public abstract class Assert {
    public static void isNotNull(Object val, String error) {
        if (val == null) {
            throw new IllegalArgumentException(error);
        }
    }

    public static void isTrue(boolean val, String error) {
        if (!val) {
            throw new IllegalArgumentException(error);
        }
    }
}
