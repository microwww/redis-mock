package com.github.microwww;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class SelectSocketsThreadPool extends SelectSockets {

    private Map<SocketChannel, TaskThread> tasks = new ConcurrentHashMap();
    private final Executor pool;

    public SelectSocketsThreadPool(Executor pool) {
        this.pool = pool;
    }

    @Override
    protected void readableHandler(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        pool.execute(() -> {
            if (!key.isValid()) {
                return;
            }
            try {
                SocketChannel k = key(channel);
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
                    tasks.remove(k, thread);
                });
            } catch (RuntimeException | IOException e) { // IO 做简单处理
                try {
                    System.out.println("Error ! try to close channel : " + e.getMessage());
                    closeChannel(key);
                } catch (IOException ex) {
                }
            }
        });
    }

    protected void readChannel(SocketChannel channel, AwaitRead lock) throws IOException {
        throw new UnsupportedOperationException("暂未实现");
    }

    public static SocketChannel key(SocketChannel channel) {
        return channel;
    }

}