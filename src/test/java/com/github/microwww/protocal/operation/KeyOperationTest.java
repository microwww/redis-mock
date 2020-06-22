package com.github.microwww.protocal.operation;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import static org.junit.Assert.*;

public class KeyOperationTest {

    private Jedis jd;

    @Before
    public void init() throws IOException {
        InetSocketAddress add = Server.startListener();
        jd = new Jedis(add.getHostName(), add.getPort(), 1000);
    }

    @Test
    public void del() throws IOException {
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        String key2 = UUID.randomUUID().toString();
        jd.set(key2, key2);
        Long count = jd.del(key1, key2, UUID.randomUUID().toString());
        assertEquals(2, count.longValue());
    }

    @Test
    public void testExpire() throws IOException {
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        int ex = (int) (10 + Math.random() * 100000);
        jd.expire(key1, ex);
        Long ttl = jd.ttl(key1);
        assertEquals(ttl, ex, 1);

        jd.expire(key1, 0);
        String val = jd.get(key1);
        assertNull(val);
    }

    @Test
    public void testTestDel() {
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        jd.expire(key1, 0);
        String ttl = jd.get(key1);
        assertNull(ttl);
    }

    @Test
    public void testExists() {
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        Boolean exists = jd.exists(key1);
        assertTrue(exists);
    }

    public void testExpireat() {
    }

    public void testKeys() {
    }

    public void testMove() {
    }

    public void testPersist() {
    }

    public void testPexpire() {
    }

    public void testPexpireat() {
    }

    public void testPttl() {
    }

    public void testRandomkey() {
    }

    public void testRename() {
    }

    public void testTestRename() {
    }

    public void testRenamenx() {
    }

    public void testSort() {
    }

    public void testType() {
    }
}