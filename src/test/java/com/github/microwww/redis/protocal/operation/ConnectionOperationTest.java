package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;

public class ConnectionOperationTest extends AbstractRedisTest {

    @Test
    public void auth() {
        String s = jedis.auth("s");
        assertEquals("OK", s);
    }

    @Test
    public void testEcho() {
        String s = jedis.echo("Hello");
        assertEquals("Hello", s);
    }

    @Test
    public void ping() {
        String s = jedis.ping();
        assertEquals("PONG", s);
    }

    @Test
    public void select() {
        String key = UUID.randomUUID().toString();
        jedis.set(key, UUID.randomUUID().toString());
        String s0 = jedis.get(key);
        assertNotNull(s0);
        String s = jedis.select(1);
        String s1 = jedis.get(key);
        assertNull(s1);
    }

    @Test
    public void testQuit() {
        String s = jedis.quit();
        assertEquals("OK", s);
        jedis.close();
    }
}