package com.github.microwww;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SelectSocketsThreadPool extends SelectSockets {
    private static final int MAX_THREADS = 5;
    private static ConcurrentLinkedQueue<ByteBuffer> queue = new ConcurrentLinkedQueue();

    private static final Executor pool = Executors.newFixedThreadPool(MAX_THREADS);

    // -------------------------------------------------------------
    public static void main(String[] argv) throws Exception {
        SelectSocketsThreadPool sst = new SelectSocketsThreadPool();
        Runnable run = sst.config("localhost", 10421);
        pool.execute(run);
        InetSocketAddress ss = (InetSocketAddress) sst.getServerSocket().getLocalSocketAddress();
        System.out.println(ss.getHostString() + ":" + ss.getPort());
    }

}