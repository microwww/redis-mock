package com.github.microwww.redis;

import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public abstract class ChannelInputStream implements Closeable {
    private static final Logger log = LogFactory.getLogger(ChannelInputStream.class);
    private final Executor threads;
    private final PipedOutputStream pout = new PipedOutputStream();
    private final PipedInputStream pin;
    private int status = 0;
    private final ChannelContext context;

    public ChannelInputStream(ChannelContext context) {
        this(context, Executors.newCachedThreadPool());
    }

    public ChannelInputStream(ChannelContext context, Executor threads) {
        this.threads = Executors.newCachedThreadPool();
        this.context = context;
        try {
            this.pin = new PipedInputStream(pout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(ByteBuffer buffer) throws IOException {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        synchronized (this) {
            if (status == 0) {
                status = 1;
                threads.execute(() -> {
                    try {
                        while (true) {
                            this.readableHandler(pin);
                            synchronized (this) {
                                if (pin.available() <= 0) {
                                    status = 0;
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Handler error, invoke Handler.exception", e);
                        context.getChannelHandler().exception(context, e);
                    }
                });
            }
            pout.write(data);// pin.available()  是线程同步的
        }
    }

    public abstract void readableHandler(InputStream inputStream) throws IOException;

    @Override
    public void close() throws IOException {
        try {
            pout.close();
        } finally {
            pin.close();
        }
    }
}
