package com.github.microwww.redis;

import java.io.IOException;

@FunctionalInterface
public interface ConsumerIO<T> {
    void accept(T lock) throws IOException;
}
