package com.github.microwww.redis;

import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.NetPacket;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.StringUtil;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Optional;
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
        Assert.isTrue(this.schema == null, "Server is running, you can not modify it, please invoke it before `getSchema`");
        this.schema = new Schema(size, operation);
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

    private RedisHandler handler(ChannelContext context) {
        return new RedisHandler(); // this
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

    public class RedisHandler extends ChannelSessionHandler.Adaptor {

        @Override
        public void readableHandler(ChannelContext context, ByteBuffer buffer) throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Get a request: {}", context.getRemoteHost());
                StringUtil.loggerBuffer(log, buffer.asReadOnlyBuffer());
            }
            while (true) {
                Optional<? extends NetPacket> parse = NetPacket.parse(buffer);
                if (parse.isPresent()) {
                    this.readableHandler(context, parse.get());
                } else break;
            }
        }

        private void readableHandler(ChannelContext context, NetPacket netPacket) throws IOException {
            RequestParams[] req = RequestParams.convert(netPacket);
            RedisRequest redisRequest = new RedisRequest(RedisServer.this, context, req);
            log.debug("Ready [{}], request: {}", redisRequest.getCommand(), context.getRemoteHost());
            //Object o = context.getSessions().get(Transaction.class.getName());
            RedisServer.this.getSchema().execute(redisRequest);
            log.debug("Over  [{}], request: {}", redisRequest.getCommand(), context.getRemoteHost());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            this.sockets.close();
        } finally {
            try {
                if (schema != null) schema.close();
            } finally {
                pool.shutdown();
            }
        }
    }
}
