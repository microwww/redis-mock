package com.github.microwww;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SelectSocketsThreadPool extends SelectSockets {
    private static final int MAX_THREADS = 5;

    private static final ConcurrentHashMap<String, TaskThread> tasks = new ConcurrentHashMap();
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
                String k = key(channel);
                TaskThread thread = new TaskThread();
                boolean doing = false;
                TaskThread tt = tasks.get(k);
                if (tt != null) {
                    doing = tt.append();
                }
                if (doing) {
                    return;
                }
                tasks.put(k, thread); // 原先已经停止, 开启新的读取
                thread.scheduling(() -> {
                    throw new UnsupportedOperationException("暂未实现");
                });
            } catch (IOException e) { // IO 做简单处理
                try {
                    closeChannel(key);
                } catch (IOException ex) {
                }
            }
        });
    }

    public static String key(SocketChannel channel) throws IOException {
        InetSocketAddress address = (InetSocketAddress) channel.getRemoteAddress();
        return address.getHostName() + ":" + address.getPort();
    }

}