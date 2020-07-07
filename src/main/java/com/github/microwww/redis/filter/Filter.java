package com.github.microwww.redis.filter;

import java.io.IOException;

@FunctionalInterface
public interface Filter<T> {

    public void doFilter(T o, FilterChain chain) throws IOException;
}