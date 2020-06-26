package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class HashOperationTest extends AbstractRedisTest {

    @Test
    public void hget() throws IOException {
        Jedis jd = jedis;
        String[] rs = Server.random(3);
        {// get null
            String hget = jd.hget(rs[0], rs[1]);
            assertNull(hget);
        }
        {// set / get
            Long hset = jd.hset(rs[0], rs[1], rs[2]);
            assertEquals(1, hset.intValue());
            hset = jd.hset(rs[0], rs[1], rs[2]);
            assertEquals(0, hset.intValue());

            String val = jd.hget(rs[0], rs[1]);
            assertEquals(rs[2], val);
        }
        {// delete
            jd.hset(rs[0], rs[1], rs[2]);
            String val = jd.hget(rs[0], rs[1]);
            assertEquals(rs[2], val);

            jd.hdel(rs[0], rs[1]);
            val = jd.hget(rs[0], rs[1]);
            assertNull(val);
        }
    }

    @Test
    public void hdel() {
        String key = UUID.randomUUID().toString();
        {
            long del = jedis.hdel(key, key + "0", key + "2", key + "1");
            assertEquals(0, del);
        }
        {
            jedis.hset(key, key + "0", "");
            long del = jedis.del(key, key + "0", key + "2", key + "1");
            assertEquals(1, del);
        }
        {
            jedis.hset(key + "1", key, "");
            long del = jedis.hdel(key + "1", key);
            assertEquals(1, del);
        }
    }

    @Test
    public void hexists() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            boolean ex = jedis.hexists(key, hk);
            assertFalse(ex);
        }
        {
            jedis.hset(key, hk, val);
            boolean ex = jedis.hexists(key, hk);
            assertTrue(ex);
        }
    }

    @Test
    public void testHget() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            String ex = jedis.hget(key, hk);
            assertNull(ex);
        }
        {
            jedis.hset(key, hk, val);
            String ex = jedis.hget(key, hk);
            assertEquals(ex, val);
        }
    }

    @Test
    public void hgetall() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            Map<String, String> ex = jedis.hgetAll(key);
            assertEquals(0, ex.size());
        }
        {
            jedis.hset(key, hk, val);
            jedis.hset(key, hk + "1", val);
            jedis.hset(key + "1", hk, val);
            Map<String, String> ex = jedis.hgetAll(key);
            assertEquals(2, ex.size());
        }
    }

    @Test
    public void hincrby() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        {
            long ex = jedis.hincrBy(key, hk, 1);
            assertEquals(1, ex);
        }
        {
            long ex = jedis.hincrBy(key, hk, 10);
            assertEquals(11, ex);
        }
    }

    @Test
    public void hincrbyfloat() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        {
            double ex = jedis.hincrByFloat(key, hk, 1.6);
            assertEquals(1.6, ex, 0.00000001);
        }
        {
            double ex = jedis.hincrByFloat(key, hk, 10.23);
            assertEquals(11.83, ex, 0.00000001);
        }
    }

    @Test
    public void hkeys() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            Set<String> ex = jedis.hkeys(key);
            assertEquals(0, ex.size());
        }
        {
            jedis.hset(key, hk, val);
            jedis.hset(key, hk + "1", val);
            jedis.hset(key + "1", hk, val);
            Set<String> ex = jedis.hkeys(key);
            assertEquals(2, ex.size());
        }
    }

    @Test
    public void hlen() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            long ex = jedis.hlen(key);
            assertEquals(0, ex);
        }
        {
            jedis.hset(key, hk, val);
            jedis.hset(key, hk + "1", val);
            jedis.hset(key + "1", hk, val);
            long ex = jedis.hlen(key);
            assertEquals(2, ex);
        }
    }

    @Test
    public void hmget() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            List<String> ex = jedis.hmget(key, hk, hk + "0");
            assertEquals(0, ex.size());
        }
        {
            jedis.hset(key, hk, val);
            jedis.hset(key, hk + "1", val);
            jedis.hset(key + "1", hk, val);
            List<String> ex = jedis.hmget(key, hk, hk + "0");
            assertEquals(2, ex.size());
            assertEquals(ex.get(0), val);
            assertNull(ex.get(1));
        }
    }

    @Test
    public void hmset() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            Map<String, String> map = new HashMap<>();
            map.put(hk + "0", val);
            map.put(hk + "1", val);
            map.put(hk + "2", val);
            String ex = jedis.hmset(key, map);
            assertEquals("OK", ex);
        }
    }

    @Test
    public void hset() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            long ex = jedis.hset(key, hk, val);
            assertEquals(1, ex);
        }
    }

    @Test
    public void hsetnx() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            long ex = jedis.hsetnx(key, hk, val);
            assertEquals(1, ex);
            ex = jedis.hsetnx(key, hk, val + "-0");
            assertEquals(0, ex);
        }
    }

    @Test
    public void hvals() {
        String key = UUID.randomUUID().toString();
        String hk = UUID.randomUUID().toString();
        String val = hk;
        {
            List<String> ex = jedis.hvals(key);
            assertEquals(0, ex.size());
        }
        {
            jedis.hset(key, hk + "0", val);
            jedis.hset(key, hk + "1", val);
            jedis.hset(key, hk + "2", val);
            List<String> ex = jedis.hvals(key);
            assertEquals(3, ex.size());
        }
    }
}