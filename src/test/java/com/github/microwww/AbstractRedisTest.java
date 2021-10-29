package com.github.microwww;

import com.github.microwww.redis.protocal.operation.Server;
import org.junit.After;
import org.junit.Before;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractRedisTest {
    protected static final ExecutorService threads = Executors.newCachedThreadPool();
    protected Jedis jedis;

    public Jedis connection() throws IOException {
        InetSocketAddress address = Server.startListener();
        return new Jedis(address.getHostName(), address.getPort(), 60_000);
        // return new Jedis("192.168.1.246", 6379, 60_000);
    }

    @Before
    public void init() throws IOException {
        jedis = connection();
    }

    @After
    public void close() {
        jedis.close();
    }
}
