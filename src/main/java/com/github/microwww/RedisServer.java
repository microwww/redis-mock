package com.github.microwww;

import com.github.microwww.database.Schema;
import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisRequest;
import com.github.microwww.protocal.RequestSession;
import redis.clients.jedis.Protocol;
import redis.clients.util.RedisInputStream;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RedisServer extends SelectSocketsThreadPool {

    private static final Executor pool = Executors.newFixedThreadPool(5);
    private final Map<SocketChannel, RequestSession> sessions = new ConcurrentHashMap();
    private Schema schema;

    public RedisServer() {
        super(pool);
    }

    public void configScheme(int size, AbstractOperation... operation) throws IOException {
        if (this.schema == null) {
            this.schema = new Schema(size, operation);
        }
    }

    public void listener(String host, int port) throws IOException {
        Runnable config = this.config(host, port);
        pool.execute(config);
    }

    @Override
    public Runnable config(String host, int port) throws IOException {
        Runnable run = super.config(host, port);
        return () -> {
            run.run();
        };
    }

    @Override
    protected void readChannel(SocketChannel channel, AwaitRead lock) throws IOException {
        RedisInputStream in = new RedisInputStream(new ChannelInputStream(channel, lock));
        while (in.available() > 0) {
            Object read = Protocol.read(in);
            ExpectRedisRequest[] req = ExpectRedisRequest.parseRedisData(read);
            this.getSchema().exec(new RedisRequest(this, channel, req));
        }
    }

    @Override
    protected void acceptHandler(SocketChannel channel) throws IOException {
        super.acceptHandler(channel);
        sessions.put(key(channel), new RequestSession());
    }

    public RequestSession getSession(SocketChannel channel) {
        return sessions.get(channel);
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
}
