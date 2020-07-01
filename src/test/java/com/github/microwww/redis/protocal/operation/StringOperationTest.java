package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.BitOP;

import java.util.List;
import java.util.UUID;

public class StringOperationTest extends AbstractRedisTest {

    @Test
    public void testGet() {
        String key = UUID.randomUUID().toString();
        String val = jedis.get(key);
        Assert.assertNull(val);
        jedis.set(key, key);
        val = jedis.get(key);
        Assert.assertEquals(val, key);
    }

    @Test
    public void testAppend() {
        String key = UUID.randomUUID().toString();
        Long len = jedis.append(key, key);
        Assert.assertEquals(len.intValue(), key.length());
        String val = jedis.get(key);
        Assert.assertEquals(val, key);

        len = jedis.append(key, key);
        Assert.assertEquals(len.intValue(), key.length() * 2);
        val = jedis.get(key);
        Assert.assertEquals(val, key + key);
    }

    @Test
    public void testBitcount() {
        String key = UUID.randomUUID().toString();
        Boolean og = jedis.setbit(key, 5, true);
        Assert.assertEquals(false, og);
        Long count = jedis.bitcount(key, 0, 5);
        Assert.assertEquals(1, count.intValue());
    }

    @Test
    public void testBitop() {
        String k1 = UUID.randomUUID().toString();
        jedis.setbit(k1, 1, true);
        jedis.setbit(k1, 2, true);
        String k2 = UUID.randomUUID().toString();
        jedis.setbit(k2, 1, true);
        jedis.setbit(k2, 3, true);
        String k3 = UUID.randomUUID().toString();
        // 0110
        // 0101
        {
            jedis.bitop(BitOP.OR, k3, k1, k2);
            Assert.assertFalse(jedis.getbit(k3, 0));
            Assert.assertTrue(jedis.getbit(k3, 1));
            Assert.assertTrue(jedis.getbit(k3, 2));
            Assert.assertTrue(jedis.getbit(k3, 3));
        }
        {
            jedis.bitop(BitOP.AND, k3, k1, k2);
            Assert.assertFalse(jedis.getbit(k3, 0));
            Assert.assertTrue(jedis.getbit(k3, 1));
            Assert.assertFalse(jedis.getbit(k3, 2));
            Assert.assertFalse(jedis.getbit(k3, 3));
        }
        {
            jedis.bitop(BitOP.XOR, k3, k1, k2);
            Assert.assertFalse(jedis.getbit(k3, 0));
            Assert.assertFalse(jedis.getbit(k3, 1));
            Assert.assertTrue(jedis.getbit(k3, 2));
            Assert.assertTrue(jedis.getbit(k3, 3));
        }
        {
            jedis.bitop(BitOP.NOT, k3, k1);
            Assert.assertTrue(jedis.getbit(k3, 0));
            Assert.assertFalse(jedis.getbit(k3, 1));
            Assert.assertFalse(jedis.getbit(k3, 2));
            Assert.assertTrue(jedis.getbit(k3, 3));
        }
    }

    @Test
    public void testDecr() {
        String key = UUID.randomUUID().toString();
        Long dec = jedis.decr(key);
        Assert.assertEquals(-1, dec.intValue());
        dec = jedis.decr(key);
        Assert.assertEquals(-2, dec.intValue());
        String s = jedis.get(key);
        Assert.assertEquals("-2", s);
    }

    @Test
    public void testDecrby() {
        String key = UUID.randomUUID().toString();
        Long dec = jedis.decrBy(key, 5);
        Assert.assertEquals(-5, dec.intValue());
        dec = jedis.decrBy(key, 5);
        Assert.assertEquals(-10, dec.intValue());
        String s = jedis.get(key);
        Assert.assertEquals("-10", s);
    }

    public void testGetbit() {
    }

    @Test
    public void testGetrange() {
        String key = UUID.randomUUID().toString();
        String dec = jedis.getrange(key, 5, 7);
        Assert.assertEquals(dec, "");
        jedis.set(key, "1234567890");
        dec = jedis.getrange(key, 5, 7);
        Assert.assertEquals("678", dec);
        dec = jedis.getrange(key, 5, -1);
        Assert.assertEquals("67890", dec);
        dec = jedis.getrange(key, -2, -1);
        Assert.assertEquals("90", dec);
    }

    @Test
    public void testGetset() {
        String key = UUID.randomUUID().toString();
        String val = jedis.getSet(key, "1");
        Assert.assertNull(val);
        val = jedis.getSet(key, "1234567890");
        Assert.assertEquals("1", val);
    }

    @Test
    public void testIncr() {
        String key = UUID.randomUUID().toString();
        long dec = jedis.incr(key);
        Assert.assertEquals(1, dec);
        dec = jedis.incr(key);
        Assert.assertEquals(2, dec);
    }

    @Test
    public void testIncrby() {
        String key = UUID.randomUUID().toString();
        long dec = jedis.incrBy(key, 10);
        Assert.assertEquals(10, dec);
        dec = jedis.incrBy(key, 1);
        Assert.assertEquals(11, dec);
    }

    @Test
    public void testIncrbyfloat() {
        String key = UUID.randomUUID().toString();
        double dec = jedis.incrByFloat(key, 5.0);
        Assert.assertEquals(5, dec, 0.00000000000000001);
        dec = jedis.incrByFloat(key, 5.000001);
        Assert.assertEquals(10.000001, dec, 0.00000000000000001);
    }

    @Test
    public void testMget() {
        String k1 = UUID.randomUUID().toString();
        String k2 = UUID.randomUUID().toString();
        String k3 = UUID.randomUUID().toString();
        List<String> mget = jedis.mget(k1, k2, k3);
        Assert.assertEquals(3, mget.size());
        Assert.assertNull(mget.get(0));
        jedis.set(k2, k2);
        mget = jedis.mget(k1, k2, k3);
        Assert.assertEquals(3, mget.size());
        Assert.assertEquals(k2, mget.get(1));
    }

    @Test
    public void testMset() {
        String k1 = UUID.randomUUID().toString();
        String k2 = UUID.randomUUID().toString();
        String k3 = UUID.randomUUID().toString();
        try {
            jedis.mset(k1, k2, k3);
            fail();
        } catch (Exception e) {
        }
        jedis.mset(k1, k2, k3, k1);
        String val = jedis.get(k1);
        Assert.assertEquals(k2, val);
        val = jedis.get(k3);
        Assert.assertEquals(k1, val);
    }

    @Test
    public void testMsetnx() {
        String k1 = UUID.randomUUID().toString();
        String k2 = UUID.randomUUID().toString();
        String k3 = UUID.randomUUID().toString();
        try {
            jedis.mset(k1, k2, k3);
            fail();
        } catch (Exception e) {
        }
        long len = jedis.msetnx(k1, k1, k2, k2);
        Assert.assertEquals(1, len);
        String val = jedis.get(k1);
        Assert.assertEquals(k1, val);
        len = jedis.msetnx(k1, k1, k3, k1);
        Assert.assertEquals(0, len);
    }

    @Test
    public void testPsetex() throws InterruptedException {
        String k1 = UUID.randomUUID().toString();
        String len = jedis.psetex(k1, 500L, k1);
        Assert.assertEquals("OK", len);
        assertEquals(k1, jedis.get(k1));
        Thread.sleep(500L);
        assertNull(jedis.get(k1));
    }

    @Test
    public void testSet() throws InterruptedException {
        String k1 = UUID.randomUUID().toString();
        String v = k1 + "-";
        {
            String nx = jedis.set(k1, k1, "xx");
            assertNull(nx);
            assertNull(jedis.get(k1));
        }
        {
            String nx = jedis.set(k1, v, "nx");
            Assert.assertEquals("OK", nx);
            assertEquals(v, jedis.get(k1));
        }
        {
            String nx = jedis.set(k1, v + "1", "nx"); // try , is not
            assertNull(nx);
            assertEquals(v, jedis.get(k1));
        }
        {
            String nx = jedis.set(k1, v + "2", "xx");
            assertEquals(v + "2", jedis.get(k1));
        }
        {
            jedis.set(k1, v, "xx", "px", 500);
            assertEquals(v, jedis.get(k1));
            Thread.sleep(500);
            assertNull(jedis.get(k1));
        }
    }

    @Test
    public void testSetex() throws InterruptedException {
        String k1 = UUID.randomUUID().toString();
        String v = k1 + "-";
        jedis.setex(k1, 500, v);
        assertEquals(v, jedis.get(k1));
        assertEquals(500, jedis.ttl(k1).doubleValue(), 1.0);
    }

    @Test
    public void testSetnx() {
        String k1 = UUID.randomUUID().toString();
        String v = k1 + "-";
        {
            long nx = jedis.setnx(k1, v);
            assertEquals(1, nx);
        }
        { // try again
            long nx = jedis.setnx(k1, v + "2");
            assertEquals(0, nx);
            assertEquals(v, jedis.get(k1));
        }
    }

    @Test
    public void testSetrange() {
        String k1 = UUID.randomUUID().toString();
        {
            long nx = jedis.setrange(k1, 2, "234");
            assertEquals(5, nx);
            assertEquals("\000\000234", jedis.get(k1));
        }
        {
            long nx = jedis.setrange(k1, 0, "1234567");
            assertEquals(7, nx);
            assertEquals("1234567", jedis.get(k1));
        }
        {
            long nx = jedis.setrange(k1, 4, "56789");
            assertEquals(9, nx);
            assertEquals("123456789", jedis.get(k1));
        }
        {
            long nx = jedis.setrange(k1, 2, "000");
            assertEquals(9, nx);
            assertEquals("120006789", jedis.get(k1));
        }
    }

    @Test
    public void testStrlen() {
        String k1 = UUID.randomUUID().toString();
        String v = k1 + "-";
        {
            long nx = jedis.strlen(k1);
            assertEquals(0, nx);
        }
        {
            jedis.set(k1, v);
            long nx = jedis.strlen(k1);
            assertEquals(v.length(), nx);
        }
    }
}