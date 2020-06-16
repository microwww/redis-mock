package com.github.microwww;

import com.github.microwww.util.Assert;
import redis.clients.jedis.Protocol;
import redis.clients.util.RedisInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RedisServer extends SelectSocketsThreadPool {
    public static final String UTF8 = "UTF-8";

    private static final int PORT = Protocol.DEFAULT_PORT;
    private static final String HOST = Protocol.DEFAULT_HOST;
    private static final int BUF_SIZE = 1024 * 8;
    private static final int TIMEOUT = 1000;

    private static final Executor pool = Executors.newFixedThreadPool(5);

    public RedisServer() {
        super(pool);
    }

    @Override
    protected void readChannel(SocketChannel channel, AwaitRead lock) throws IOException {
        RedisInputStream in = new RedisInputStream(Channels.newInputStream(channel));
        while (in.available() > 0) {
            Object read = Protocol.read(in);
            if(read == null){
                continue;
            }
            if (read instanceof byte[]) {
                String commend = new String((byte[]) read, UTF8);
            }
            if (read instanceof Collection) {
                Collection list = (Collection) read;
            }
            if (read instanceof Number) {
                long val = ((Number) read).longValue();
            }
            throw new UnsupportedOperationException("未完待续");
        }
    }
}
