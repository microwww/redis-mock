package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

import static org.junit.Assert.*;

public class EVALOperationTest extends AbstractRedisTest {

    @Test
    public void evalString() {
        Jedis jd = jedis;

        String[] key = Server.random(1);
        String[] val = Server.random(1);
        Object a = jedis.eval(
                "redis.call('SET',KEYS[1],ARGV[1]);" +
                        "return redis.call('GET',KEYS[1]);",
                Arrays.asList(key),
                Arrays.asList(val));

        assertEquals(val[0], a);
    }

    @Test
    public void eval() {

        String[] key = Server.random(3);
        String[] val = Server.random(4);
        Object a = jedis.eval("local a = KEYS[3];" +
                        "redis.call('RPUSH',ARGV[1],a);" +
                        "return redis.call('RPOP',ARGV[1]);",
                Arrays.asList(key),
                Arrays.asList(val));

        assertEquals(key[2], a);
    }

    @Test
    public void evalListSize() {

        String[] key = Server.random(3);
        String[] val = Server.random(4);
        Object a = jedis.eval("local a = KEYS[3];" +
                        "redis.call('RPUSH',ARGV[1],a);" +
                        "redis.call('RPUSH',ARGV[1],KEYS[1]);" +
                        "return redis.call('LLEN',ARGV[1]);",
                Arrays.asList(key),
                Arrays.asList(val));

        assertEquals("2", a);
    }

    @Test
    public void evalHashSize() {

        String[] key = Server.random(3);
        String[] val = Server.random(4);
        Object a = jedis.eval("local a = KEYS[3];" +
                        "redis.call('HSET',ARGV[1],a,KEYS[1]);" +
                        "redis.call('HSET',ARGV[1],KEYS[1],a);" +
                        "return redis.call('HLEN',ARGV[1]);",
                Arrays.asList(key),
                Arrays.asList(val));

        assertEquals("2", a);
    }

    @Test
    public void evalHashGet() {

        String[] key = Server.random(3);
        String[] val = Server.random(4);
        Object a = jedis.eval(
                        "redis.call('HSET',ARGV[1],KEYS[1],KEYS[2]);" +
                        "return redis.call('HGET',ARGV[1],KEYS[1]);",
                Arrays.asList(key),
                Arrays.asList(val));

        assertEquals(key[1], a);
    }
}
