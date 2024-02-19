package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class EVALOperationTest extends AbstractRedisTest {

    @Test
    public void evalString() {
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
    public void evalFor() {

        String[] key = Server.random(3);
        String[] val = Server.random(4);
        Object a = jedis.eval("redis.call('SADD',KEYS[1],ARGV[1]);" +
                        "redis.call('SADD',KEYS[1],ARGV[2]);" +
                        "redis.call('SADD',KEYS[1],ARGV[3]);" +
                        "local a = redis.call('SMEMBERS',KEYS[1]);" +
                        "local b = 0;" +
                        "for i,v in ipairs(a) do" +
                        "   b = b + i;" +
                        "end;" +
                        "return b;",
                Arrays.asList(key),
                Arrays.asList(val));

        assertEquals(6L, a);
    }

    @Test
    public void evalSetAll() {

        String[] key = Server.random(3);
        String[] val = Server.random(4);
        Object a = jedis.eval(
                        "redis.call('SADD',ARGV[1],KEYS[1]);" +
                        "redis.call('SADD',ARGV[1],KEYS[2]);" +
                        "return redis.call('SMEMBERS',ARGV[1]);",
                Arrays.asList(key),
                Arrays.asList(val));

        List res = (List) a;

        assertTrue(res.contains(key[0]));
        assertTrue(res.contains(key[1]));
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

        assertEquals(2L, a);
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

        assertEquals(2L, a);
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
