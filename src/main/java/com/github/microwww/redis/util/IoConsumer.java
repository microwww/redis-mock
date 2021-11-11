package com.github.microwww.redis.util;

import java.io.IOException;
import java.util.Objects;

@FunctionalInterface
public interface IoConsumer<T> {
    void accept(T o) throws IOException;

    default IoConsumer<T> andThen(IoConsumer<? super T> after) {
        Objects.requireNonNull(after);
        return (T t) -> {
            accept(t);
            after.accept(t);
        };
    }
}