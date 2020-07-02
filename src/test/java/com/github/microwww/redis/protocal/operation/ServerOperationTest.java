package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ServerOperationTest extends AbstractRedisTest {

    @Test
    public void testDbsize() {
        long size = jedis.dbSize();
        assertTrue(size > 0);
    }

    @Test
    public void testFlushall() {
        String[] r = Server.random(10);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.select(1);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.select(2);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.select(3);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.flushAll();
        {
            Set<String> keys;
            jedis.select(0);
            keys = jedis.keys("*");
            assertEquals(0, keys.size());
            jedis.select(1);
            keys = jedis.keys("*");
            assertEquals(0, keys.size());
            jedis.select(2);
            keys = jedis.keys("*");
            assertEquals(0, keys.size());
            jedis.select(3);
            keys = jedis.keys("*");
            assertEquals(0, keys.size());
        }
    }

    @Test
    public void testFlushdb() {
        String[] r = Server.random(10);
        jedis.select(1);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.flushDB();
        {
            Set<String> keys = jedis.keys("*");
            assertEquals(0, keys.size());
        }
    }

    @Test
    public void testTime() {
        List<String> time = jedis.time();
        assertEquals(10, time.get(0).length());
        assertTrue(Integer.parseInt(time.get(1)) <= 999_999);
    }
}