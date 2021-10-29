package com.github.microwww.redis;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ChannelContextTest {

    @Test
    public void close() throws IOException, InterruptedException {
        AtomicInteger c = new AtomicInteger(0);
        int count = 10;
        ChannelContext ctx = new ChannelContext(null);
        for (int i = 0; i < count; i++) {
            ctx.addCloseListener(e -> {
                c.incrementAndGet();
            });
        }
        ctx.close();
        Thread.sleep(1000);
        assertEquals(count, c.get());
        ctx.close();
        assertEquals(count, c.get());
    }
}