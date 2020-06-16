package com.github.microwww;

import java.io.IOException;

public interface Reading {
    void read(AwaitRead lock) throws IOException;
}
