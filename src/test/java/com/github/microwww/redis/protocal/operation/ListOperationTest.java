package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.junit.Assert.*;

public class ListOperationTest extends AbstractRedisTest {

    @Test
    public void testList() {
        String[] r = Server.random(6);

        {
            try {
                jedis.lset(r[0], 0, r[1]);
                fail();
            } catch (JedisDataException e) {
            }
        }
        {
            String key = r[0];
            Long rpush = jedis.rpush(key, r[1]);
            assertEquals(1, rpush.intValue());
            String v = jedis.lindex(key, 0);
            assertEquals(r[1], v);

            rpush = jedis.rpush(key, r[1], r[2], r[3]);
            assertEquals(4, rpush.intValue());
        }
        {
            String key = r[5];
            Long rpush = jedis.rpush(key, r[1]);
            assertEquals(1, rpush.intValue());
            String v = jedis.rpop(key);
            assertEquals(r[1], v);

            rpush = jedis.rpush(key, r[1]);
            assertEquals(1, rpush.intValue());
        }
    }
}