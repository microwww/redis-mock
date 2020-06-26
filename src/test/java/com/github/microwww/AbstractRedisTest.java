package com.github.microwww;

import com.github.microwww.redis.protocal.operation.Server;
import org.junit.Before;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;

public abstract class AbstractRedisTest {

    protected Jedis jedis;

    @Before
    public void init() throws IOException {
        InetSocketAddress address = Server.startListener();
        jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
    }
}
