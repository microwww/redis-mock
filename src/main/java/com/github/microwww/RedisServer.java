package com.github.microwww;

import com.github.microwww.protocal.RedisRequest;
import com.github.microwww.protocal.RequestSession;
import com.github.microwww.protocal.ServiceProtocol;
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

    public RedisServer() {
        super(pool);
    }

    public void listener(String host, int port) throws IOException {
        Runnable config = this.config(host, port);
        pool.execute(config);
    }

    @Override
    protected void readChannel(SocketChannel channel, AwaitRead lock) throws IOException {
        RedisInputStream in = new RedisInputStream(new ChannelInputStream(channel, lock));
        while (in.available() > 0) {
            Object read = Protocol.read(in);
            ExpectRedisRequest[] req = ExpectRedisRequest.parseRedisData(read);
            ServiceProtocol.exec(new RedisRequest(this, channel, req));
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
}
