package com.github.microwww.protocal.operation;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import static org.junit.Assert.*;

public class KeyOperationTest {

    @Test
    public void del() throws IOException {
        InetSocketAddress add = Server.startListener();
        Jedis jd = new Jedis(add.getHostName(), add.getPort(), 1000);
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        String key2 = UUID.randomUUID().toString();
        jd.set(key2, key2);
        Long count = jd.del(key1, key2, UUID.randomUUID().toString());
        assertEquals(2, count.longValue());
    }
}