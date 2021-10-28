package com.github.microwww.redis.util;

import java.util.List;

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

    public static void isNotEmpty(List<?> val, String error) {
        if (val == null || val.isEmpty()) {
            throw new IllegalArgumentException(error);
        }
    }

    public static void isTrue(boolean val, String error) {
        if (!val) {
            throw new IllegalArgumentException(error);
        }
    }

}
