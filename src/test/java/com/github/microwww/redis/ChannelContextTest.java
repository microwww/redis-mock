package com.github.microwww.redis;

import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisRequest;
import org.junit.Test;
import org.mockito.Mockito;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

public class ChannelContextTest {

    @Test(timeout = 1000)
    public void testChannelContextCloseListener() throws IOException, InterruptedException {
        CountDownLatch d1 = new CountDownLatch(1);
        CountDownLatch d2 = new CountDownLatch(1);
        RedisServer server = new RedisServer();
        server.configScheme(16, new AbstractOperation() {
            // test close
            public void echo(RedisRequest request) throws IOException {
                request.expectArgumentsCount(1);
                byte[] echo = request.getParams()[0].getByteArray();
                request.getOutputProtocol().writer(echo);
                ChannelContext ctx = request.getContext();
                if ("client-close".equals(new String(echo))) {
                    ctx.addCloseListener0(() -> {
                        d1.countDown();
                    });
                } else if ("server-close".equals(new String(echo))) {
                    try {
                        request.setNext(() -> {
                        });
                        ctx.closeChannel();
                    } finally {
                        d2.countDown();
                    }
                }
            }
        });
        server.listener("localhost", 0);
        SocketAddress addr = server.getSockets().getServerSocket().getLocalSocketAddress();
        InetSocketAddress address = (InetSocketAddress) addr;
        {
            Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
            jedis.echo("client-close");
            jedis.close();
            d1.await(); // close 方法会被调用
        }
        {
            Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 60_000);
            String sc = "server-close";
            String echo = jedis.echo(sc);
            d2.await(); // 可正常返回
            assertEquals(sc, echo);
            try {
                jedis.echo(sc);
                fail();
            } catch (JedisConnectionException e) {
            }
        }
    }

    @Test
    public void testBuffer() {
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(new byte[]{0, 1, 2, 3, 4, 5});
        assertEquals(6, bf.position());
        bf.flip();
        assertEquals(0, bf.get());
        assertEquals(1, bf.position());
        ByteBuffer b2 = ByteBuffer.allocate(1024);
        b2.put(bf);
        assertEquals(5, b2.position());
        b2.flip();
        assertEquals(1, b2.get());
    }

    @Test
    public void testRead() throws IOException {
        SocketChannel sk = Mockito.mock(SocketChannel.class);
        Mockito.when(sk.read(Mockito.any(ByteBuffer.class))).then(inv -> {
            ByteBuffer bf = inv.getArgument(0);
            byte[] bts = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
            bf.put(bts);
            return bts.length;
        });
        Mockito.when(sk.getRemoteAddress())
                .thenReturn(new InetSocketAddress("localhost", 2022));
        ChannelContext context = new ChannelContext(sk);
        ByteBuffer chn = context.readChannel();
        chn.get();// read one
        context.readOver(chn);
        ByteBuffer b2 = context.readChannel();
        int capacity = b2.capacity();
        assertEquals(0, b2.position());
        // read one and next is `2 * 9 - 1`
        assertEquals(17, b2.limit());
        b2.clear().flip();
        context.readOver(b2);

        Mockito.when(sk.read(Mockito.any(ByteBuffer.class))).then(inv -> {
            ByteBuffer bf = inv.getArgument(0);
            int size = bf.limit() - bf.position();
            for (int i = 0; i < size; i++) {
                bf.put((byte) i);
            }
            return size;
        });
        b2 = context.readChannel();
        context.readOver(b2);

        b2 = context.readChannel(); // over capacity will up `2 * capacity`
        assertEquals(2 * capacity, b2.capacity());

        b2.clear().flip();
        context.readOver(b2);// clear

        b2 = context.readChannel();
        assertEquals(capacity, b2.capacity());
    }
}