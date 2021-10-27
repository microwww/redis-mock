package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.util.List;

public class TransactionOperationTest extends AbstractRedisTest {

    @Test(timeout = 1000)
    public void pipelined() {
        Pipeline tr = jedis.pipelined();
        tr.set("test0".getBytes(), "TEST".getBytes());
        tr.set("test1".getBytes(), "TEST".getBytes());
        tr.get("test1".getBytes());
        List<Object> exec = tr.syncAndReturnAll();
        Assert.assertEquals(3, exec.size());
    }

    @Test
    public void testExec() {
        Transaction tr = jedis.multi();
        List<Object> exec = tr.exec();
        Assert.assertTrue(exec.isEmpty());
    }

    @Test
    public void testMulti() {
        String[] r = Server.random(10);
        Transaction tr = jedis.multi();
        tr.set(r[0], r[0]);
        tr.set(r[1], r[0]);
        tr.set(r[2], r[0]);
        List<Object> exec = tr.exec();
        Assert.assertEquals(3, exec.size());
    }

    @Test
    public void testDiscard() {
        String[] r = Server.random(8);
        String key = r[0];
        Transaction tr = jedis.multi();
        tr.set(key, "");
        tr.discard();
        try {
            tr.exec();
            Assert.fail("Not hear");
        } catch (JedisDataException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void testUnwatch() throws IOException {
        String[] r = Server.random(8);
        {
            String key = r[0];
            jedis.set(key, "");
            jedis.watch(key);
            Transaction tr = jedis.multi();
            this.connection().expire(key, 1000L);
            // jedis.unwatch(); // jedis-3.0 会被忽略, jedis-3.6+ client 直接报错
            tr.set(key, "");
            List<Object> exec = tr.exec();
            AssertMultiError(exec);
        }
        {
            String key = r[1];
            jedis.set(key, "");
            Transaction tr = jedis.multi();
            this.connection().expire(key, 1000L);
            //jedis.unwatch(); // 同上, jedis-3.0 会被忽略
            tr.set(key, "");
            try {
                tr.exec();
            } catch (NullPointerException ex) {
                // redis 2.9 error !
            }
        }
        {
            String key = r[1];
            jedis.set(key, "");
            jedis.watch(key);
            Transaction tr = jedis.multi();
            tr.set(key, "");
            List<Object> exec = tr.exec();
            Assert.assertEquals(1, exec.size());
        }
    }

    @Test
    public void testWatch() throws IOException {
        String[] r = Server.random(8);
        //jedis = new Jedis("192.168.1.246");
        {
            String key = r[0];
            jedis.watch(key);
            Transaction tr = jedis.multi();
            this.connection().expire(key, 1000L); // 无值
            tr.set(key, "");
            List<Object> exec = tr.exec();
            Assert.assertEquals(1, exec.size());
        }
        {
            String key = r[1];
            jedis.set(key, "");
            jedis.watch(key);
            Transaction tr = jedis.multi();
            this.connection().expire(key, 1000L);
            tr.set(key, "");
            List<Object> exec = tr.exec();
            AssertMultiError(exec);
        }
        {
            String key = r[2];
            jedis.hset(key, key, "");
            jedis.watch(key);
            Transaction tr = jedis.multi();
            this.connection().hset(key, key, ""); // change is error, equal is same also
            tr.set(key, "");
            List<Object> exec = tr.exec();
            AssertMultiError(exec);
        }
        {
            String key = r[3];
            jedis.hset(key, key, "");
            jedis.watch(key);
            Transaction tr = jedis.multi();
            this.connection().hset(key, key, ""); // change is error, equal is same also
            tr.set(key, "");
            List<Object> exec = tr.exec();
            AssertMultiError(exec);
        }

    }

    private void AssertMultiError(List<Object> exec) {
        try {
            Assert.assertEquals(0, exec.size()); // 2.0+ return empty list
        } catch (NullPointerException e) {
            Assert.assertNull(exec); // 3.0+ return null
        }
    }
}