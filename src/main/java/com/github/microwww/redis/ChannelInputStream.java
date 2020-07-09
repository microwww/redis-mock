package com.github.microwww.redis;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ChannelInputStream extends InputStream {
    private final ByteBuffer buffer;
    private final SocketChannel channel;
    private final AwaitRead lock;

    public ChannelInputStream(SocketChannel channel, AwaitRead lock) {
        this(channel, lock, 1024 * 8);
    }

    public ChannelInputStream(SocketChannel channel, AwaitRead lock, int buffer) {
        this.channel = channel;
        this.lock = lock;
        this.buffer = ByteBuffer.allocate(buffer);
        // 起始置空
        this.buffer.flip();
    }

    private synchronized int tryRead(boolean block) throws IOException {
        int remaining = buffer.remaining();
        while (remaining <= 0) {
            buffer.clear();
            int len = channel.read(buffer);
            buffer.flip();
            if (len == 0) {
                if (!block) {
                    return len;
                }
                lock.park(); // block
                continue;
            }
            return len;
        }
        return remaining;
    }

    @Override
    public int available() throws IOException {
        int len = tryRead(false);
        return Math.max(len, 0);
    }

    @Override
    public int read() throws IOException { // will block
        int len = tryRead(true);
        if (len < 0) { // 远程正常关闭连接
            return -1;
        }
        return buffer.get();
    }

    @Override
    public int read(byte[] bts, int off, int len) throws IOException { // no block !!!
        int r = tryRead(false);
        if (r > 0) {
            buffer.get(bts, off, Math.min(r, len));
        }
        return r;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
