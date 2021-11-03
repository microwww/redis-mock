package com.github.microwww.redis;

import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ChannelContextTest {

    @Test
    public void testChannelContextCloseListener() throws IOException, InterruptedException {
        AtomicInteger c = new AtomicInteger(0);
        CountDownLatch d1 = new CountDownLatch(1);
        CountDownLatch d2 = new CountDownLatch(1);
        RedisServer server = new RedisServer();
        server.configScheme(16, new AbstractOperation() {
            // test close
            public void echo(RedisRequest request) throws IOException {
                request.expectArgumentsCount(1);
                byte[] echo = request.getParams()[0].getByteArray();
                RedisOutputProtocol.writer(request.getOutputStream(), echo);
                ChannelContext ctx = request.getContext();
                ctx.addCloseListener(e -> {
                    c.incrementAndGet();
                    d1.countDown();
                });
                if ("server-close".equals(new String(echo))) {
                    ctx.closeChannel();
                    c.incrementAndGet();
                    d2.countDown();
                }
            }
        });
        server.listener("localhost", 0);
        SocketAddress addr = server.getSockets().getServerSocket().getLocalSocketAddress();
        InetSocketAddress address = (InetSocketAddress) addr;
        {
            Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
            jedis.echo("i will close");
            jedis.close();
            d1.await();
            assertEquals(1, c.get());
        }
        {
            Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
            jedis.echo("server-close");
            d2.await();
            assertEquals(2, c.get());
        }
    }
}