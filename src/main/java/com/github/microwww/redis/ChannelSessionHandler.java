package com.github.microwww.redis;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * like netty, one ChannelSessionHandler for one SocketChannel
 */
public interface ChannelSessionHandler {

    void registerHandler(ChannelContext context) throws IOException;

    void readableHandler(ChannelContext context, ByteBuffer buffer) throws IOException;

    void exception(ChannelContext context, Exception ex);

    void close(ChannelContext context) throws IOException;

    class Sharable {
    }

    class UnSharable {
    }

    class Adaptor implements ChannelSessionHandler {

        @Override
        public void registerHandler(ChannelContext context) throws IOException {
        }

        @Override
        public void readableHandler(ChannelContext context, ByteBuffer buffer) throws IOException {
        }

        @Override
        public void exception(ChannelContext context, Exception ex) {
        }

        @Override
        public void close(ChannelContext context) throws IOException {
        }
    }
}
