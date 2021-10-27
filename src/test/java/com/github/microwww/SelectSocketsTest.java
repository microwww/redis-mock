package com.github.microwww;

import com.github.microwww.redis.*;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.jedis.JedisInputStream;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class SelectSocketsTest {
    public static final Logger log = LogFactory.getLogger(RedisServer.class);

    @Test
    public void testInputStream() throws Exception {
        ExecutorService pool = Executors.newCachedThreadPool();
        int count = 5;
        CountDownLatch d1 = new CountDownLatch(count);
        CountDownLatch d2 = new CountDownLatch(count);
        List<String> bf = new CopyOnWriteArrayList<>();
        SelectSockets sockets = new SelectSockets();
        sockets.bind("localhost", 0);
        pool.execute(() -> {
            sockets.startListener((e) -> {
                return new ChannelSessionHandler.Adaptor() {
                    private ChannelInputStream channelInputStream;

                    @Override
                    public void registerHandler(ChannelContext context) throws IOException {
                        channelInputStream = new ChannelInputStream(context) {
                            @Override
                            public void readableHandler(InputStream inputStream) throws IOException {
                                read(context, inputStream);
                            }
                        };
                    }

                    @Override
                    public void readableHandler(ChannelContext context, ByteBuffer buffer) throws IOException {
                        channelInputStream.write(buffer);
                    }

                    private void read(ChannelContext context, InputStream inputStream) throws IOException {
                        JedisInputStream in = new JedisInputStream(inputStream);
                        Object read = in.readRedisData();
                        ExpectRedisRequest[] req = ExpectRedisRequest.parseRedisData(read);
                        bf.add(new String(req[0].isNotNull().getByteArray())); // 命令
                        d1.countDown();
                    }
                };
            });
        });
        InetSocketAddress ss = (InetSocketAddress) sockets.getServerSocket().getLocalSocketAddress();
        for (int i = 0; i < count; i++) {
            pool.execute(() -> {
                try {
                    Jedis jedis = new Jedis(ss.getHostString(), ss.getPort(), 10000);
                    d2.countDown();
                    jedis.ping();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        d1.await();
        d2.await();
        assertEquals(count, bf.size());
    }

    @Test(timeout = 1000)
    public void testChannelHandler() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(5);
        SelectSockets sockets = new SelectSockets();
        sockets.bind("localhost", 0);
        int count = 10;
        CountDownLatch stop = new CountDownLatch(count);
        CountDownLatch runs = new CountDownLatch(count);
        CountDownLatch doing = new CountDownLatch(count);
        pool.execute(() -> {
            sockets.startListener(new ChannelSessionHandler.Adaptor() {
                @Override
                public void registerHandler(ChannelContext context) throws IOException {
                    runs.countDown();
                }

                @Override
                public void readableHandler(ChannelContext context, ByteBuffer bf) throws IOException {
                    byte[] bt = new byte[bf.remaining()];
                    bf.get(bt);
                    String v = String.format("context %12d, %s", System.identityHashCode(context), new String(bt));
                    throw new RuntimeException(v);
                }

                @Override
                public void exception(ChannelContext context, Exception ex) {
                    log.info("From readableHandler RuntimeException, {}", ex.getMessage());
                    doing.countDown();
                }

                @Override
                public void close(ChannelContext context) throws IOException {
                    log.info("Stop ChannelContext : {}", String.format("%12d", System.identityHashCode(context)));
                    stop.countDown();
                }
            });
        });

        InetSocketAddress ss = (InetSocketAddress) sockets.getServerSocket().getLocalSocketAddress();

        List<Socket> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Socket sk = new Socket();
            sk.connect(new InetSocketAddress(ss.getHostName(), ss.getPort()), 1000);
            sk.getOutputStream().write("hello".getBytes(StandardCharsets.UTF_8));
            sk.getOutputStream().flush();
            list.add(sk);
        }
        runs.await();
        doing.await();
        list.forEach(e -> {
            try {
                e.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });
        stop.await();
    }
}