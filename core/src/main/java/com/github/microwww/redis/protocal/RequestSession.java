package com.github.microwww.redis.protocal;

import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class RequestSession extends ConcurrentHashMap<String, Object> implements Closeable {
    public static final String ADDRESS = RequestSession.class.getName() + ".ADDRESS";
    public static final String NAME = RequestSession.class.getName() + ".NAME";
    private final SocketChannel channel;
    private int database = 0;

    public RequestSession(SocketChannel channel) {
        this.channel = channel;
        //this.put(ADDRESS, RedisServer.addressKey(channel));
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        Assert.isTrue(database >= 0, "database >= 0");
        this.database = database;
    }

    @NotNull
    public Optional<String> getName() {
        return Optional.ofNullable((String) this.get(NAME));
    }

    public void setName(String name) {
        this.put(NAME, name);
    }

    @NotNull
    public String getAddress() {
        return withDefault(ADDRESS, "127.0.0.1:6379");
    }

    public <T> T withDefault(String key, T def) {
        Assert.isNotNull(def, "Not null");
        return (T) this.getOrDefault(key, def);
    }

    @Override
    public void close() {
        this.clear();
    }
}
