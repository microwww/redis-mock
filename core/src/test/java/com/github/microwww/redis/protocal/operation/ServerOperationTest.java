package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.FlushMode;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

public class ServerOperationTest extends AbstractRedisTest {

    @Test
    public void testDbsize() {
        long origin = jedis.dbSize();
        // assertEquals(size, 0);
        jedis.set(UUID.randomUUID().toString(), "");
        long size = jedis.dbSize();
        assertEquals(origin + 1, size);
    }

    @Test
    public void testFlushall() {
        String[] r = Server.random(10);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.select(1);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.select(2);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.select(3);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.flushAll();
        {
            Set<String> keys;
            jedis.select(0);
            keys = jedis.keys("*");
            assertEquals(0, keys.size());
            jedis.select(1);
            keys = jedis.keys("*");
            assertEquals(0, keys.size());
            jedis.select(2);
            keys = jedis.keys("*");
            assertEquals(0, keys.size());
            jedis.select(3);
            keys = jedis.keys("*");
            assertEquals(0, keys.size());
        }
        jedis.flushAll(FlushMode.ASYNC);
    }

    @Test
    public void testFlushdb() {
        String[] r = Server.random(10);
        jedis.select(1);
        jedis.set(r[0], r[0]);
        {
            Set<String> keys = jedis.keys("*");
            assertTrue(keys.size() > 0);
        }
        jedis.flushDB();
        jedis.flushDB(FlushMode.ASYNC);
        {
            Set<String> keys = jedis.keys("*");
            assertEquals(0, keys.size());
        }
    }

    @Test
    public void testTime() {
        List<String> time = jedis.time();
        assertEquals(10, time.get(0).length());
        assertTrue(Integer.parseInt(time.get(1)) <= 999_999);
    }

    @Test
    public void testClient() throws Exception {
        // jedis = connection();
        String clients = jedis.clientList();
        Socket socket = jedis.getClient().getSocket();
        InetSocketAddress add = (InetSocketAddress) socket.getLocalSocketAddress();
        String ip = add.getHostName() + ":" + add.getPort();
        assertTrue(clients.contains(ip));
        jedis.clientSetname("test");
        assertEquals("test", jedis.clientGetname());

        jedis.clientKill(ip);
        try {
            while (true) {
                jedis.ping();
            }
        } catch (JedisConnectionException ex) {
            assertNotNull(ex);
        }
        Jedis j2 = connection();
        clients = j2.clientList();
        assertFalse(clients.contains(ip));
    }
}