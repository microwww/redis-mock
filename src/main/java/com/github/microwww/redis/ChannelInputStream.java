package com.github.microwww.redis;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public abstract class ChannelInputStream {
    private static final Executor threads = Executors.newCachedThreadPool();
    private final PipedOutputStream pout = new PipedOutputStream();
    private final PipedInputStream pin;
    private int status = 0;
    private final ChannelContext context;

    public ChannelInputStream(ChannelContext context) {
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
                    } catch (IOException e) {
                        context.getChannelHandler().exception(context, e);
                    }
                });
            }
            pout.write(data);// pin.available()  是线程同步的
        }
    }

    public abstract void readableHandler(InputStream inputStream) throws IOException;
}
