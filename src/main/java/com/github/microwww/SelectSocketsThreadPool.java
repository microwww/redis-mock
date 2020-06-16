package com.github.microwww;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SelectSocketsThreadPool extends SelectSockets {

    private ConcurrentHashMap<String, TaskThread> tasks = new ConcurrentHashMap();
    private final Executor pool;

    public SelectSocketsThreadPool(Executor pool) {
        this.pool = pool;
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
                thread.scheduling((lock) -> {
                    readChannel(channel, lock);
                });
            } catch (RuntimeException | IOException e) { // IO 做简单处理
                try {
                    System.out.println("Error close channel :" + e.getMessage());
                    closeChannel(key);
                } catch (IOException ex) {
                }
            }
        });
    }

    protected void readChannel(SocketChannel channel, AwaitRead lock) throws IOException {
        throw new UnsupportedOperationException("暂未实现");
    }

    public static String key(SocketChannel channel) throws IOException {
        InetSocketAddress address = (InetSocketAddress) channel.getRemoteAddress();
        return address.getHostName() + ":" + address.getPort();
    }

}