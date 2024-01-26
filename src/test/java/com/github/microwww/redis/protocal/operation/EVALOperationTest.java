package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

public class EVALOperationTest extends AbstractRedisTest {
    @Test
    public void eval() {
        Jedis jd = jedis;
        Object a = jedis.eval("local a = KEYS[3];local b = ARGV[4];redis.call('RPUSH',ARGV[1],'123','233');return redis.call('LPOP',ARGV[1]);", Arrays.asList("kk","k2","k3"), Arrays.asList("v1","v2","v3","v4"));
        Object aa = jedis.eval("local a = KEYS[3];local b = ARGV[4];return redis.call('LPOP',ARGV[1]);", Arrays.asList("kk","k2","k3"), Arrays.asList("v1","v2","v3","v4"));

        System.out.println("::::::::::"+a);
        System.out.println("::::::::::"+aa);
    }

    @Test
    public void evalList() {
        Jedis jd = jedis;
        Object a = jedis.eval("local key = KEYS[3];" +
                "local field = ARGV[4];" +
                "local value = ARGV[1];" +
                "redis.call('HSET',key,field,value);" +
                "redis.call('HSET',key,field..1,value);" +
                "redis.call('HSET',key..1,field,value);" +
                "local keys = redis.call('HKEYS',key);return #keys", Arrays.asList("kk","k2","k3"), Arrays.asList("v1","v2","v3","v4"));

        System.out.println("::::::::::"+a);
    }

    @Test
    public void evalList2() {
        Jedis jd = jedis;
        Object a = jedis.eval("local key = KEYS[3];" +
                "local field = ARGV[4];" +
                "local value = ARGV[1];" +
                "redis.call('HSET',key,field,value);" +
                "redis.call('HSET',key,field..1,value);" +
                "redis.call('HSET',key..1,field,value);" +
                "local keys = redis.call('HGETALL',key);print(keys[1]..'|'..keys[2]..'|'..keys[3]..'|'..keys[4]);print(#keys)", Arrays.asList("kk","k2","k3"), Arrays.asList("v1","v2","v3","v4"));

        System.out.println("::::::::::"+a);
    }
}
