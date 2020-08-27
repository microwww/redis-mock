package com.github.microwww.redis.protocal.jedis;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.RedisInputStream;

import java.io.IOException;
import java.io.InputStream;

public class JedisInputStream extends InputStream {

    private final RedisInputStream redisInputStream;

    public JedisInputStream(InputStream in) {
        redisInputStream = new RedisInputStream(in);
    }

    public Object readRedisData() {
        return Protocol.read(redisInputStream);
    }

    @Override
    public int read() throws IOException {
        return redisInputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return redisInputStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws JedisConnectionException {
        return redisInputStream.read(b, off, len);
    }

    @Override
    public int available() throws IOException {
        return redisInputStream.available();
    }

    @Override
    public void close() throws IOException {
        redisInputStream.close();
    }
}
