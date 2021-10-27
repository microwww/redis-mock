package com.github.microwww.redis;

import com.github.microwww.redis.protocal.RequestSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ChannelContext {
    private final SocketChannel channel;
    private ChannelSessionHandler channelHandler;
    private final RequestSession sessions;
    private final ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    public ChannelContext(SocketChannel channel) {
        this.channel = channel;
        this.sessions = new RequestSession(channel);
    }

    public ChannelSessionHandler getChannelHandler() {
        return channelHandler;
    }

    void setChannelHandler(ChannelSessionHandler channelHandler) {
        this.channelHandler = channelHandler;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public RequestSession getSessions() {
        return sessions;
    }

    public ByteBuffer readChannel() throws IOException {
        buffer.clear();
        int read = channel.read(buffer);
        if (read < 0) {
            throw new IOException("EOF");
        }
        buffer.flip();
        return buffer.asReadOnlyBuffer();
    }
}
