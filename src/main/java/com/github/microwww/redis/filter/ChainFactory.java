package com.github.microwww.redis.filter;

import java.io.IOException;

public class ChainFactory<T> {

    private final Filter[] filters;

    public ChainFactory(Filter<T>... filters) {
        if (filters == null) {
            filters = new Filter[]{};
        }
        this.filters = filters;
    }

    /**
     * FilterChain 非线程安全的
     */
    public FilterChain<T> create() {
        return new FilterChain<T>() {
            private int index = -1;

            @Override
            public void doFilter(T o) throws IOException {
                index++;
                if (index < filters.length) {
                    filters[index].doFilter(o, this);
                }
            }

        };
    }
}