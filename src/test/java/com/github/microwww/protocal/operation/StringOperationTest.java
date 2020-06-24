package com.github.microwww.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.BitOP;

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

    public void testDecr() {
    }

    public void testDecrby() {
    }

    public void testGetbit() {
    }

    public void testGetrange() {
    }

    public void testGetset() {
    }

    public void testIncr() {
    }

    public void testIncrby() {
    }

    public void testIncrbyfloat() {
    }

    public void testMget() {
    }

    public void testMset() {
    }

    public void testMsetnx() {
    }

    public void testPsetex() {
    }

    public void testSet() {
    }

    public void testSetex() {
    }

    public void testSetnx() {
    }

    public void testSetrange() {
    }

    public void testStrlen() {
    }
}