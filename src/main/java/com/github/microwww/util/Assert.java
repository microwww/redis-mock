package com.github.microwww.util;

public abstract class Assert {

    public static void allNotNull(String error, Object... val) {
        if (val == null) {
            throw new IllegalArgumentException(error);
        }
        for (Object v : val) {
            if (v == null) {
                throw new IllegalArgumentException(error);
            }
        }
    }

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

    public static void notHere(String message, @NotNull Exception e) {
        throw new RuntimeException(message, e);
    }
}
