package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
}