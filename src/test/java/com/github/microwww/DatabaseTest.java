package com.github.microwww;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DatabaseTest {

    @Test
    public void testConnection() throws IOException {
        RedisServer redisServer = new RedisServer();
        redisServer.listener("localhost", 0);
        InetSocketAddress add = (InetSocketAddress) redisServer.getServerSocket().getLocalSocketAddress();

        Jedis jd = new Jedis(add.getHostName(), add.getPort());
        String ping = jd.ping();
        Assert.assertEquals(ping, Protocol.Keyword.PONG.name());
        jd.close();
    }
}