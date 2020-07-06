package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
                assertNotNull(e);
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

    @Test
    public void lset() {
    }

    @Test
    public void rpush() {
        String[] r = Server.random(8);
        jedis.rpush(r[0], r[1], r[2], r[3]);

        String rpop = jedis.rpop(r[0]);
        assertEquals(r[3], rpop);
    }

    @Test
    public void rpop() {
    }

    @Test
    public void blpop() throws InterruptedException, IOException {
        String[] r = Server.random(8);
        InetSocketAddress address = Server.startListener();
        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>();
        new Thread(() -> {
            try {
                Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
                List<String> pop = jedis.blpop(1000, r[0], r[1], r[3]);
                queue.put(pop);
            } catch (Exception ex) {
                assertNotNull(ex);
            }
        }).start();
        rpush(r, address).start();
        List<String> take = queue.take();
        assertFalse(take.isEmpty());
    }

    @Test
    public void blpopOver() throws InterruptedException, IOException {
        String[] r = Server.random(8);
        InetSocketAddress address = Server.startListener();
        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>();
        new Thread(() -> {
            try {
                Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
                List<String> pop = jedis.blpop(5, r[0], r[1], r[3]);
                queue.put(pop);
            } catch (Exception ex) {
                assertNotNull(ex);
            }
        }).start();
        List<String> take = queue.take();
        assertTrue(take.isEmpty());
    }

    private Thread rpush(String[] r, InetSocketAddress address) {
        return new Thread(() -> {
            Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
            jedis.rpush(r[0], r[4]);
            jedis.rpush(r[1], r[4]);
            jedis.rpush(r[2], r[4]);
        });
    }

    @Test
    public void brpoplpush() throws Exception {
        String[] r = Server.random(8);
        InetSocketAddress address = Server.startListener();
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        new Thread(() -> {
            try {
                Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
                queue.put(r[3]);
                String pop = jedis.brpoplpush(r[0], r[1], 2);
                queue.put(pop);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }).start();
        String take = queue.take();
        assertEquals(r[3], take);
        Thread.sleep(100);
        rpush(r, address).start();
        take = queue.take();
        assertNotNull(take);
    }

    @Test
    public void lindex() {
        String[] r = Server.random(8);
        String data = jedis.lindex(r[0], 1);
        assertNull(data);
        jedis.rpush(r[0], r[1]);
        data = jedis.lindex(r[0], 1);
        assertNull(data);
        data = jedis.lindex(r[0], 0);
        assertEquals(r[1], data);
    }

    @Test
    public void linsert() {
        String[] r = Server.random(8);
        long val = jedis.linsert(r[0], BinaryClient.LIST_POSITION.AFTER, r[1], r[2]);
        assertEquals(0, val);
        jedis.rpush(r[0], r[6]);
        val = jedis.linsert(r[0], BinaryClient.LIST_POSITION.BEFORE, r[1], r[3]);// R3, R1, R2
        assertEquals(-1, val);
        val = jedis.linsert(r[0], BinaryClient.LIST_POSITION.AFTER, r[6], r[4]);// R6, R4
        assertEquals(2, val);
        val = jedis.linsert(r[0], BinaryClient.LIST_POSITION.BEFORE, r[6], r[2]);// R2, R6, R4
        assertEquals(3, val);
        assertEquals(r[2], jedis.lindex(r[0], 0));
        assertEquals(r[6], jedis.lindex(r[0], 1));
        assertEquals(r[4], jedis.lindex(r[0], 2));

        //    public void llen() {
        assertEquals(3, jedis.llen(r[0]).intValue());
    }

    @Test
    public void llen() {
        String[] r = Server.random(8);
        assertEquals(0, jedis.llen(r[0]).intValue());
        jedis.rpush(r[0], r[6]);
        assertEquals(1, jedis.llen(r[0]).intValue());
    }

    @Test
    public void lpop() {
        String[] r = Server.random(8);
        assertNull(jedis.lpop(r[0]));
        jedis.lpush(r[0], r[6]);
        assertEquals(r[6], jedis.lpop(r[0]));
    }

    @Test
    public void lpush() {// up-up-up
    }

    @Test
    public void lpushx() {
        String[] r = Server.random(8);
        assertEquals(0, jedis.llen(r[0]).intValue());
        jedis.lpushx(r[0], r[6]);
        assertEquals(0, jedis.llen(r[0]).intValue());
        jedis.lpush(r[0], r[1]);
        jedis.lpushx(r[0], r[6]);
        assertEquals(r[6], jedis.lpop(r[0]));
    }

    @Test
    public void lrange() {
        String[] r = Server.random(8);
        assertEquals(0, jedis.lrange(r[0], 0, -1).size());
        jedis.rpush(r[0], r[1], r[2], r[3], r[4], r[5], r[6]);
        assertEquals(6, jedis.llen(r[0]).intValue());
        assertEquals(r[3], jedis.lrange(r[0], 2, 2).get(0));
        assertEquals(6, jedis.lrange(r[0], 0, -1).size());
    }

    @Test
    public void lrem() {
        String[] r = Server.random(8);
        assertEquals(0, jedis.lrange(r[0], 0, -1).size());
        jedis.rpush(r[0], r[1], r[2], r[3], r[4], r[5], r[6]);
        assertEquals(6, jedis.llen(r[0]).intValue());
        assertEquals(r[3], jedis.lrange(r[0], 2, 2).get(0));
        assertEquals(6, jedis.lrange(r[0], 0, -1).size());
    }

    @Test
    public void ltrim() {
        String[] r = Server.random(8);
        String ok = jedis.ltrim(r[0], 1, 5);
        assertEquals("OK", ok);
        jedis.rpush(r[0], r[1], r[2], r[3], r[4], r[5], r[6]);
        ok = jedis.ltrim(r[0], 1, -1);
        assertEquals("OK", ok);
        assertEquals(5, jedis.llen(r[0]).intValue());
        ok = jedis.ltrim(r[0], 1, 2);
        assertEquals("OK", ok);
        assertEquals(2, jedis.llen(r[0]).intValue());
        assertEquals(r[3], jedis.lpop(r[0]));
        assertEquals(r[4], jedis.lpop(r[0]));
    }

    @Test
    public void rpoplpush() {
        String[] r = Server.random(8);
        String rpp = jedis.rpoplpush(r[0], r[1]);
        assertNull(rpp);
        jedis.lpush(r[0], r[5], r[6]);
        rpp = jedis.rpoplpush(r[0], r[1]);
        assertEquals(r[5], rpp);
    }

    @Test
    public void rpushx() {
        String[] r = Server.random(8);
        assertEquals(0, jedis.llen(r[0]).intValue());
        jedis.rpushx(r[0], r[6]);
        assertEquals(0, jedis.llen(r[0]).intValue());
        jedis.lpush(r[0], r[1]);
        jedis.rpushx(r[0], r[6]);
        assertEquals(r[1], jedis.lpop(r[0]));
    }
}