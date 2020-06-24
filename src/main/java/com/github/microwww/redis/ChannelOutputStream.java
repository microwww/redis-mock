package com.github.microwww.redis;

import com.github.microwww.redis.util.Assert;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ChannelOutputStream extends OutputStream {
    private final ByteBuffer buffer;
    private final SocketChannel channel;

    public ChannelOutputStream(SocketChannel channel) {
        this(channel, 1024 * 8);
    }

    public ChannelOutputStream(SocketChannel channel, int buffer) {
        Assert.isTrue(buffer > 0, "Must buffer size > 0");
        this.channel = channel;
        this.buffer = ByteBuffer.allocate(buffer);
    }

    @Override
    public void write(int b) throws IOException {
        if (buffer.remaining() == 0) {
            this.flush();
        }
        buffer.put((byte) b);
    }

    @Override
    public void flush() throws IOException {
        buffer.flip();
        channel.write(buffer);
        buffer.clear();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
