package com.github.microwww.redis.protocal.operation;

import java.util.UUID;

import com.github.microwww.AbstractRedisTest;

import static org.junit.Assert.*;
import org.junit.Test;

public class HyperLogLogTest extends AbstractRedisTest {

    @Test
    public void testPfCount() {
        String key = UUID.randomUUID().toString();
        jedis.pfadd(key, "elements");
        jedis.pfadd(key, "elements1");
        assertEquals(2, jedis.pfcount(key, key + "0"));

        jedis.pfadd(key + "0", "elements");
        assertEquals(2, jedis.pfcount(key, key + "0"));

        jedis.pfadd(key + "0", "elements2");
        assertEquals(3, jedis.pfcount(key, key + "0"));
    }

    @Test
    public void testPfMerge() {
        String key = UUID.randomUUID().toString();
        jedis.pfadd(key, "elements");
        jedis.pfadd(key, "elements1");
        jedis.pfadd(key + "0", "elements");
        jedis.pfadd(key + "0", "elements2");
        jedis.pfmerge(key + "1", key, key + "0");
        assertEquals(3, jedis.pfcount(key + "1"));
    }
}
