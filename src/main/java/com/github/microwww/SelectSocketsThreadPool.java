package com.github.microwww;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class SelectSocketsThreadPool extends SelectSockets {
    private static final int MAX_THREADS = 5;

    private static final Executor pool = Executors.newFixedThreadPool(MAX_THREADS);

    // -------------------------------------------------------------
    public static void main(String[] argv) throws Exception {
        SelectSocketsThreadPool sst = new SelectSocketsThreadPool();
        Runnable run = sst.config("localhost", 10421);
        pool.execute(run);
        InetSocketAddress ss = (InetSocketAddress) sst.getServerSocket().getLocalSocketAddress();
        System.out.println(ss.getHostString() + ":" + ss.getPort());
    }

    @Override
    protected void readableHandler(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        pool.execute(() -> {
            try {
                TaskThread.addTask(channel);
            } catch (IOException e) {
                try {
                    closeChannel(key);
                } catch (IOException ex) {
                }
            }
        });
    }

}