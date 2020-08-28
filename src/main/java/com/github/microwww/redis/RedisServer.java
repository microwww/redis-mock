package com.github.microwww.redis;

import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.filter.Filter;
import com.github.microwww.redis.filter.FilterChain;
import com.github.microwww.redis.filter.ChainFactory;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.*;
import com.github.microwww.redis.protocal.jedis.JedisInputStream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RedisServer extends SelectSocketsThreadPool {
    public static final Logger log = LogFactory.getLogger(RedisServer.class);

    private static final Executor pool = Executors.newFixedThreadPool(5);
    private final Map<String, RequestSession> sessions = new ConcurrentHashMap<>();
    private Schema schema;
    private static final List<Filter> filters = new CopyOnWriteArrayList<>();

    public RedisServer() {
        super(pool);
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
        Runnable config = this.config(host, port);

        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {//
                log.error("Thread runtime error {}", e);
            });
        }
        pool.execute(config);
    }

    @Override
    public Runnable config(String host, int port) throws IOException {
        Runnable run = super.config(host, port);
        return () -> {
            InetSocketAddress address = (InetSocketAddress) this.serverSocket.getLocalSocketAddress();
            log.info("Redis server start @ {}:{}", address.getHostName(), "" + address.getPort());
            // RUN and block !
            run.run();
        };
    }

    @Override
    protected void readChannel(SocketChannel channel, AwaitRead lock) throws IOException {
        JedisInputStream in = new JedisInputStream(new ChannelInputStream(channel, lock));
        while (in.available() > 0) {
            Object read = in.readRedisData();
            ExpectRedisRequest[] req = ExpectRedisRequest.parseRedisData(read);
            RedisRequest redisRequest = new RedisRequest(this, channel, req);
            redisRequest.setInputStream(in);

            Filter[] filters = this.appendToArray((r, chain) -> {//
                RedisServer.this.getSchema().exec(redisRequest);
            });
            FilterChain<RedisRequest> fc = new ChainFactory<RedisRequest>(filters).create();
            fc.doFilter(redisRequest);

        }
    }

    @Override
    protected void acceptHandler(SocketChannel channel) throws IOException {
        super.acceptHandler(channel);
        sessions.put(addressKey(channel), new RequestSession(channel));
    }

    public RequestSession getSession(SocketChannel channel) throws IOException {
        return sessions.get(addressKey(channel));
    }

    public Optional<RequestSession> getSession(String channel) {
        return Optional.ofNullable(sessions.get(channel));
    }

    public Map<String, RequestSession> getSessions() {
        return sessions;
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

    public static String addressKey(SocketChannel client) throws IOException {
        InetSocketAddress rm = (InetSocketAddress) client.getRemoteAddress();
        return (rm.getHostName() + ":" + rm.getPort()).toLowerCase();
    }
}
