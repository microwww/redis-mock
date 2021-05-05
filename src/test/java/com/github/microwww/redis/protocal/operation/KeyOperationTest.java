package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

public class KeyOperationTest extends AbstractRedisTest {

    @Test
    public void del() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        String key2 = UUID.randomUUID().toString();
        jedis.set(key2, key2);
        Long count = jedis.del(key1, key2, UUID.randomUUID().toString());
        assertEquals(2, count.longValue());
    }

    @Test
    public void unlink() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        String key2 = UUID.randomUUID().toString();
        jedis.set(key2, key2);
        // 异步删除 服务器端使用同步删除
        Long count = jedis.unlink(key1, key2, UUID.randomUUID().toString());
        assertEquals(2, count.longValue());
    }

    @Test
    public void testExpire() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        int ex = (int) (10 + Math.random() * 100000);
        jedis.expire(key1, ex);
        Long ttl = jedis.ttl(key1);
        assertEquals(ttl, ex, 1);

        jedis.expire(key1, 0);
        String val = jedis.get(key1);
        assertNull(val);
    }

    @Test
    public void testTestDel() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        jedis.expire(key1, 0);
        String ttl = jedis.get(key1);
        assertNull(ttl);
    }

    @Test
    public void testExists() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        Boolean exists = jedis.exists(key1);
        assertTrue(exists);
        Long i = jedis.exists(key1, key1 + "0");
        assertEquals(i.longValue(), 1);
        // 0
        i = jedis.exists(key1 + "1", key1 + "0");
        assertEquals(i.longValue(), 0);
    }

    @Test
    public void testExpireat() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        long time = 1000;
        List<String> ts = jedis.time();
        long current = Long.parseLong(ts.get(0)) * 1000 + (Long.parseLong(ts.get(1)) / 1000);
        jedis.expireAt(key1, current / 1000 + time);
        Long t = jedis.ttl(key1);
        assertEquals(time, t, 1.0);
    }

    @Test
    public void testKeys() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        //jd.select(8);
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        jedis.set(key1 + "0", key1);
        jedis.set(key1 + "1", key1);
        jedis.set(key1 + "2" + key1, key1);
        Set<String> keys = jedis.keys("*");
        assertTrue(keys.size() >= 4);
        assertTrue(keys.contains(key1));

        keys = jedis.keys(key1 + "*");
        assertEquals(keys.size(), 4);

        keys = jedis.keys(key1 + "?");
        assertEquals(keys.size(), 2);

        keys = jedis.keys(key1 + "??");
        assertEquals(keys.size(), 0);

        keys = jedis.keys(key1 + "[01]*");
        assertEquals(keys.size(), 2);
    }

    @Test
    public void testMove() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        String key1 = UUID.randomUUID().toString();
        Long move = jedis.move(key1, 2);
        assertEquals(0, move.intValue());
        jedis.set(key1, key1);
        move = jedis.move(key1, 2);
        assertEquals(1, move.intValue());
        assertFalse(jedis.exists(key1));
        jedis.select(2);
        assertEquals(key1, jedis.get(key1));
    }

    @Test
    public void testPersist() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        jedis.expire(key1, 1000);
        jedis.persist(key1);
        Long ttl = jedis.ttl(key1);
        assertEquals(-1, ttl.intValue());
    }

    @Test
    public void testPexpire() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        long ex = (long) (10 + Math.random() * 100000);
        jedis.pexpire(key1, ex);
        Long ttl = jedis.ttl(key1);
        assertEquals(ttl, (int) (ex / 1000), 1.0);
        Long t2 = jedis.pttl(key1);
        // This test depends on the computer configuration
        assertEquals(ttl * 1000, t2, 1000);

        jedis.pexpire(key1, 0L);
        String val = jedis.get(key1);
        assertNull(val);
    }

    @Test
    public void testPexpireat() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(key1, key1);
        long time = 100 * 1000;
        List<String> ts = jedis.time();
        long current = Long.parseLong(ts.get(0)) * 1000 + (Long.parseLong(ts.get(1)) / 1000);
        jedis.pexpireAt(key1, current + time);
        Long t = jedis.pttl(key1);
        assertEquals(time, t, 1000.0);

        jedis.pexpireAt(key1, 0);
        assertFalse(jedis.exists(key1));
    }

    @Test
    public void testPttl() {
        String key1 = UUID.randomUUID().toString();
        Long t = jedis.pttl(key1);
        assertEquals(-2, t.intValue());
    }

    @Test
    public void testRandomkey() {
        String key1 = UUID.randomUUID().toString();
        jedis.set(UUID.randomUUID().toString(), key1);
        jedis.set(UUID.randomUUID().toString(), key1);
        jedis.set(UUID.randomUUID().toString(), key1);
        jedis.set(UUID.randomUUID().toString(), key1);
        String s = jedis.randomKey();
        assertNotNull(s);
    }

    @Test
    public void testRename() {
        //Jedis jd = new Jedis("192.168.2.18");
        //jd.auth("123456");
        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        try {
            jedis.rename(key1, key2);
            fail();
        } catch (JedisDataException ex) {
            assertNotNull(ex);
        }
        {
            jedis.set(key1, key1);
            String rename = jedis.rename(key1, key2);
            assertEquals("OK", rename);
            String val = jedis.get(key2);
            assertEquals(key1, val);
        }
        {
            jedis.set(key1, key2);
            String rename = jedis.rename(key1, key2);
            assertEquals("OK", rename);
            String val = jedis.get(key2);
            assertEquals(key2, val); // over-write
        }
    }

    @Test
    public void testRenamenx() {
        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        try {
            jedis.rename(key1, key2);
            fail();
        } catch (JedisDataException ex) {
            assertNotNull(ex);
        }
        {
            jedis.set(key1, key1);
            long rename = jedis.renamenx(key1, key2);
            assertEquals(1, rename);
            String val = jedis.get(key2);
            assertEquals(key1, val);
        }
        {
            jedis.set(key1, key2);
            long rename = jedis.renamenx(key1, key2);
            assertEquals(0, rename);
            String val = jedis.get(key2);
            assertEquals(key1, val);
        }
    }

    public void testSort() {
    }

    @Test
    public void testType() {
        String[] r = Server.random(10);
        String key1 = r[0];
        jedis.set(key1, "");
        String type = jedis.type(key1);
        assertEquals("string", type);
        jedis.set(key1.getBytes(), "1".getBytes());
        type = jedis.type(key1);
        assertEquals("string", type);

        jedis.lpush(r[1], r[1]);
        type = jedis.type(r[1]);
        assertEquals("list", type);

        jedis.hset(r[2], r[1], r[1]);
        type = jedis.type(r[2]);
        assertEquals("hash", type);

        jedis.sadd(r[3], r[1]);
        type = jedis.type(r[3]);
        assertEquals("set", type);

        jedis.zadd(r[4], 0, r[1]);
        type = jedis.type(r[4]);
        assertEquals("zset", type);

        type = jedis.type(key1 + "0");
        assertEquals("none", type);
    }

    @Test(timeout = 3000)
    public void testScan() {
        String[] r = Server.random(25);
        jedis.select(7);
        for (int i = 0; i < r.length; i++) {
            jedis.set(r[i], i + "", new SetParams().nx().ex(10));
        }
        String cursor = "0";
        int size = 0;
        while (true) {
            ScanResult<String> scan = jedis.scan(cursor);
            cursor = scan.getCursor();
            size += scan.getResult().size();
            if ("0".equalsIgnoreCase(cursor)) {
                break;
            }
        }
        assertEquals(25, size);
    }
}