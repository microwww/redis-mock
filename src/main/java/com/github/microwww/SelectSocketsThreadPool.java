package com.github.microwww;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class SelectSocketsThreadPool extends SelectSockets {
    private static final int MAX_THREADS = 5;

    private static ConcurrentHashMap<String, TaskThread> tasks = new ConcurrentHashMap();
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
        String conn = getKey(channel);
        pool.execute(() -> {
            TaskThread thread = new TaskThread(1, Thread.currentThread());
            TaskThread def = tasks.putIfAbsent(conn, thread);
            boolean doing = false;
            if (def != null) {
                doing = def.goon();
            }
            if (!doing) {
                System.out.println("Doing ......... ");
            }
        });
    }

    public static class TaskThread {
        private final AtomicInteger count;
        private final Thread thread;

        public TaskThread(int count, Thread thread) {
            this.count = new AtomicInteger(count);
            this.thread = thread;
        }

        public AtomicInteger getCount() {
            return count;
        }

        public Thread getThread() {
            return thread;
        }

        public boolean goon() {
            return false;
        }
    }
}