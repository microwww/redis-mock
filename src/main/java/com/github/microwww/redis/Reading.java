package com.github.microwww.redis;

import java.io.IOException;

public interface Reading {
    void read(AwaitRead lock) throws IOException;
}
