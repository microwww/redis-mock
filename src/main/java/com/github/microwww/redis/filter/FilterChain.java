package com.github.microwww.redis.filter;

import java.io.IOException;

@FunctionalInterface
public interface FilterChain<T> {
    public void doFilter(T o) throws IOException;
}