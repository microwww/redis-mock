package com.github.microwww.lettuce;

import com.github.microwww.redis.protocal.operation.Server;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractLettuceTest {
    protected static final ExecutorService threads = Executors.newCachedThreadPool();
    private static InetSocketAddress address;

    static {
        try {
            address = Server.startListener();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static RedisClient client;

    protected StatefulRedisConnection<String, String> redis;// = client.connect();

    public static RedisClient client() {
        return RedisClient.create("redis://" + address.getHostName() + ":" + address.getPort() + "/0");
        // return RedisClient.create("redis://192.168.1.246:6379/0");
    }

    @BeforeClass
    public static void init0() throws IOException {
        client = client();
    }

    @AfterClass
    public static void close0() {
        client.shutdownAsync();
    }

    @Before
    public void init() throws IOException {
        redis = client.connect();
    }

    @After
    public void close() {
        if (redis != null) redis.close();
    }
}
