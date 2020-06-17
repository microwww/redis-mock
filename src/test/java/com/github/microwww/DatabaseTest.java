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
        String result = jd.ping();
        Assert.assertEquals(result, Protocol.Keyword.PONG.name());
        result = jd.select(1);
        Assert.assertEquals(result, Protocol.Keyword.OK.name());
        result = jd.set("test", "daatata");
        Assert.assertEquals(result, Protocol.Keyword.OK.name());
        jd.close();
    }
}