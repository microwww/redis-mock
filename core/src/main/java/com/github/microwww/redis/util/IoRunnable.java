package com.github.microwww.redis.util;

import java.io.IOException;
import java.util.Objects;

@FunctionalInterface
public interface IoRunnable {
    void run() throws IOException;

    default IoRunnable andThen(IoRunnable after) {
        Objects.requireNonNull(after);
        return () -> {
            run();
            after.run();
        };
    }
}