package com.github.microwww.protocal.operation;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
        Long i = jd.exists(key1, key1 + "0");
        assertEquals(i.longValue(), 1);
        // 0
        i = jd.exists(key1 + "1", key1 + "0");
        assertEquals(i.longValue(), 0);
    }

    @Test
    public void testExpireat() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        long time = 1000;
        jd.expireAt(key1, System.currentTimeMillis() / 1000 + time);
        Long t = jd.ttl(key1);
        assertEquals(time, t, 1.0);
    }

    @Test
    public void testKeys() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        //jd.select(8);
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        jd.set(key1 + "0", key1);
        jd.set(key1 + "1", key1);
        jd.set(key1 + "2" + key1, key1);
        Set<String> keys = jd.keys("*");
        assertTrue(keys.size() >= 4);
        keys.contains(key1);

        keys = jd.keys(key1 + "*");
        assertEquals(keys.size(), 4);

        keys = jd.keys(key1 + "?");
        assertEquals(keys.size(), 2);

        keys = jd.keys(key1 + "??");
        assertEquals(keys.size(), 0);

        keys = jd.keys(key1 + "[01]*");
        assertEquals(keys.size(), 2);
    }

    @Test
    public void testMove() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        String key1 = UUID.randomUUID().toString();
        Long move = jd.move(key1, 2);
        assertEquals(0, move.intValue());
        jd.set(key1, key1);
        move = jd.move(key1, 2);
        assertEquals(1, move.intValue());
        assertFalse(jd.exists(key1));
        jd.select(2);
        assertEquals(key1, jd.get(key1));
    }

    @Test
    public void testPersist() {
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        jd.expire(key1, 1000);
        jd.persist(key1);
        Long ttl = jd.ttl(key1);
        assertEquals(-1, ttl.intValue());
    }

    @Test
    public void testPexpire() {
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        long ex = (long) (10 + Math.random() * 100000);
        jd.pexpire(key1, ex);
        Long ttl = jd.ttl(key1);
        assertEquals(ttl, ex / 1000, 1.0);
        Long t2 = jd.pttl(key1);
        assertTrue(ttl * 1000 < t2);

        jd.pexpire(key1, 0L);
        String val = jd.get(key1);
        assertNull(val);
    }

    @Test
    public void testPexpireat() {
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, key1);
        long time = 100 * 1000;
        jd.pexpireAt(key1, System.currentTimeMillis() + time);
        Long t = jd.pttl(key1);
        assertEquals(time, t, 1000.0);

        jd.pexpireAt(key1, 0);
        assertFalse(jd.exists(key1));
    }

    @Test
    public void testPttl() {
        String key1 = UUID.randomUUID().toString();
        Long t = jd.pttl(key1);
        assertEquals(-2, t.intValue());
    }

    @Test
    public void testRandomkey() {
        String key1 = UUID.randomUUID().toString();
        jd.set(UUID.randomUUID().toString(), key1);
        jd.set(UUID.randomUUID().toString(), key1);
        jd.set(UUID.randomUUID().toString(), key1);
        jd.set(UUID.randomUUID().toString(), key1);
        String s = jd.randomKey();
        assertNotNull(s);
    }

    @Test
    public void testRename() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        try {
            jd.rename(key1, key2);
            fail();
        } catch (JedisDataException ex) {
        }
        {
            jd.set(key1, key1);
            String rename = jd.rename(key1, key2);
            assertEquals("OK", rename);
            String val = jd.get(key2);
            assertEquals(key1, val);
        }
        {
            jd.set(key1, key2);
            String rename = jd.rename(key1, key2);
            assertEquals("OK", rename);
            String val = jd.get(key2);
            assertEquals(key2, val); // over-write
        }
    }

    @Test
    public void testRenamenx() {
        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        try {
            jd.rename(key1, key2);
            fail();
        } catch (JedisDataException ex) {
        }
        {
            jd.set(key1, key1);
            long rename = jd.renamenx(key1, key2);
            assertEquals(1, rename);
            String val = jd.get(key2);
            assertEquals(key1, val);
        }
        {
            jd.set(key1, key2);
            long rename = jd.renamenx(key1, key2);
            assertEquals(0, rename);
            String val = jd.get(key2);
            assertEquals(key1, val);
        }
    }

    public void testSort() {
    }

    @Test
    public void testType() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        String key1 = UUID.randomUUID().toString();
        jd.set(key1, "");
        String type = jd.type(key1);
        assertEquals("string", type);
        jd.set(key1.getBytes(), "1".getBytes());
        type = jd.type(key1);
        assertEquals("string", type);


        type = jd.type(key1 + "0");
        assertEquals("none", type);
    }
}