package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Transaction;

import java.util.List;

public class TransactionOperationTest extends AbstractRedisTest {

    public void testExec() {
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
}