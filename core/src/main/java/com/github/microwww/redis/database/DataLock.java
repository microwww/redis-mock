package com.github.microwww.redis.database;

import java.util.function.Supplier;

public interface DataLock {
    default public <T> T sync(Supplier<T> fun) {
        return fun.get();
    }
}
