package com.github.microwww.redis;

import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.filter.ChainFactory;
import com.github.microwww.redis.filter.Filter;
import com.github.microwww.redis.filter.FilterChain;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.JedisInputStream;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RedisServer {
    public static final Logger log = LogFactory.getLogger(RedisServer.class);

    private final Executor pool;
    private static final List<Filter> filters = new CopyOnWriteArrayList<>();
    private final SelectSockets sockets = new SelectSockets();
    private Schema schema;

    public RedisServer() {
        this(5);
    }

    public RedisServer(int max) {
        this(Executors.newFixedThreadPool(max));
        Assert.isTrue(max > 2, "pool > 2");
    }

    public RedisServer(Executor pool) {
        this.pool = pool;
    }

    public void configScheme(int size, AbstractOperation... operation) {
        if (this.schema == null) {
            this.schema = new Schema(size, operation);
        }
    }

    public void addFilter(Filter filter) {
        synchronized (filters) {
            filters.add(filter);
        }
    }

    public void removeFilter(Filter filter) {
        synchronized (filters) {
            filters.remove(filter);
        }
    }

    private Filter[] appendToArray(Filter f) {
        synchronized (filters) {
            Filter[] filters = RedisServer.filters.toArray(new Filter[RedisServer.filters.size() + 1]);
            filters[filters.length - 1] = f;
            return filters;
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

        pool.execute(() -> {
            sockets.startListener(channelContext -> {
                return new InputStreamHandler(channelContext); // this
            });
        });
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

        public InputStreamHandler(ChannelContext channelContext) {
            this.channelInputStream = new ChannelInputStream(channelContext) {
                @Override
                public void readableHandler(InputStream inputStream) throws IOException {
                    InputStreamHandler.this.readableHandler(channelContext, inputStream);
                }
            };
        }

        @Override
        public void readableHandler(ChannelContext context, ByteBuffer buffer) throws IOException {
            if (log.isDebugEnabled()) {
                StringUtil.loggerBuffer(buffer.asReadOnlyBuffer());
            }
            channelInputStream.write(buffer);
        }

        private void readableHandler(ChannelContext context, InputStream inputStream) throws IOException {
            JedisInputStream in = new JedisInputStream(inputStream);
            while (in.available() > 0) {
                Object read = in.readRedisData();
                ExpectRedisRequest[] req = ExpectRedisRequest.parseRedisData(read);
                RedisRequest redisRequest = new RedisRequest(RedisServer.this, context, req);
                redisRequest.setInputStream(in);
                Filter[] filters = appendToArray((r, chain) -> {//
                    RedisServer.this.getSchema().exec(redisRequest);
                });
                FilterChain<RedisRequest> fc = new ChainFactory<RedisRequest>(filters).create();
                fc.doFilter(redisRequest);
            }
        }
    }
}
