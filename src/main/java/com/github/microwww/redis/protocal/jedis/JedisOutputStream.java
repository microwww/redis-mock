package com.github.microwww.redis.protocal.jedis;

import com.github.microwww.redis.util.SafeEncoder;

import java.io.IOException;
import java.io.OutputStream;

public final class JedisOutputStream extends OutputStream {

    private final RedisOutputStream redisOutputStream;

    public JedisOutputStream(OutputStream out) {
        redisOutputStream = new RedisOutputStream(out);
    }

    public void write(byte data) throws IOException {
        redisOutputStream.write(data);
    }

    public void writeIntCrLf(int data) throws IOException {
        redisOutputStream.writeIntCrLf(data);
    }

    public void writeAsciiCrLf(String message) throws IOException {
        this.write(SafeEncoder.encode(message));
        this.writeCrLf();
    }

    public void writeCrLf() throws IOException {
        redisOutputStream.writeCrLf();
    }

    @Override
    public void write(int b) throws IOException {
        redisOutputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        redisOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        redisOutputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        redisOutputStream.flush();
    }

    @Override
    public void close() throws IOException {
        redisOutputStream.close();
    }
}
