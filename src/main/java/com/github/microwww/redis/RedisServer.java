package com.github.microwww.redis;

import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.JedisInputStream;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.StringUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer implements Closeable {
    public static final Logger log = LogFactory.getLogger(RedisServer.class);

    private final ExecutorService pool;
    private final SelectSockets sockets = new SelectSockets(this::handler);
    private Schema schema;

    public RedisServer() {
        this(5);
    }

    public RedisServer(int max) {
        this(Executors.newFixedThreadPool(max));
        Assert.isTrue(max > 2, "pool > 2");
    }

    public RedisServer(ExecutorService pool) {
        this.pool = pool;
    }

    public void configScheme(int size, AbstractOperation... operation) {
        if (this.schema == null) {
            this.schema = new Schema(size, operation);
        }
    }

    public void listener(String host, int port) throws IOException {
        sockets.bind(host, port);
        // Runnable config = this.bind(host, port);

        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {//
                log.error("Thread runtime error {}", e);
            });
        }

        ServerSocket ss = sockets.getServerSocket();
        InetSocketAddress address = (InetSocketAddress) ss.getLocalSocketAddress();
        log.info("Redis server start @ {}:{}", address.getHostName(), "" + address.getPort());

        pool.execute(sockets::sync);
    }

    private InputStreamHandler handler(ChannelContext context) {
        return new InputStreamHandler(context); // this
    }

    public Schema getSchema() {
        if (schema == null) {
            synchronized (this) {
                if (schema == null) {
                    schema = new Schema(Schema.DEFAULT_SCHEMA_SIZE);
                }
            }
        }
        return schema;
    }

    public SelectSockets getSockets() {
        return sockets;
    }

    public class InputStreamHandler extends ChannelSessionHandler.Adaptor {

        private final ChannelInputStream channelInputStream;

        public InputStreamHandler(ChannelContext context) {
            this.channelInputStream = new ChannelInputStream(context) {
                @Override
                public void readableHandler(InputStream inputStream) throws IOException {
                    log.debug("Start a request: {}", context.getRemoteHost());
                    InputStreamHandler.this.readableHandler(context, inputStream);
                }
            };
        }

        @Override
        public void readableHandler(ChannelContext context, ByteBuffer buffer) throws IOException {
            if (log.isDebugEnabled()) {
                StringUtil.loggerBuffer(buffer.asReadOnlyBuffer());
            }
            log.debug("Get a request: {}", context.getRemoteHost());
            channelInputStream.write(buffer);
        }

        private void readableHandler(ChannelContext context, InputStream inputStream) throws IOException {
            JedisInputStream in = new JedisInputStream(inputStream);
            while (in.available() > 0) {
                Object read = in.readRedisData();
                ExpectRedisRequest[] req = ExpectRedisRequest.parseRedisData(read);
                RedisRequest redisRequest = new RedisRequest(RedisServer.this, context, req);
                redisRequest.setInputStream(in);
                log.debug("Ready [{}], request: {}", redisRequest.getCommand(), context.getRemoteHost());
                RedisServer.this.getSchema().exec(redisRequest);
                log.debug("Over  [{}], request: {}", redisRequest.getCommand(), context.getRemoteHost());
            }
        }

        @Override
        public void close(ChannelContext context) throws IOException {
            channelInputStream.close();
        }

    }

    @Override
    public void close() throws IOException {
        try {
            this.sockets.close();
        } finally {
            try {
                schema.close();
            } finally {
                pool.shutdown();
            }
        }
    }
}
