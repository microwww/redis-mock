package com.github.microwww.protocal.operation;

import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import static org.junit.Assert.*;

public class ConnectionOperationTest {

    private static InetSocketAddress add;

    @BeforeClass
    public static void start() throws IOException {
        add = Server.startListener();
    }

    @Test
    public void auth() throws IOException {
        Jedis jedis = new Jedis(add.getHostName(), add.getPort());
        String s = jedis.auth("s");
        assertEquals("OK", s);
    }

    @Test
    public void ping() {
        Jedis jedis = new Jedis(add.getHostName(), add.getPort());
        String s = jedis.ping();
        assertEquals("PONG", s);
    }

    @Test
    public void select() {
        Jedis jedis = new Jedis(add.getHostName(), add.getPort());
        String key = UUID.randomUUID().toString();
        jedis.set(key, UUID.randomUUID().toString());
        String s0 = jedis.get(key);
        assertNotNull(s0);
        String s = jedis.select(1);
        String s1 = jedis.get(key);
        assertNull(s1);
    }
}