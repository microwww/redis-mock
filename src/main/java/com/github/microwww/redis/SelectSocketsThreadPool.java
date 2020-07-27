package com.github.microwww.redis;

import com.github.microwww.redis.exception.Run;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class SelectSocketsThreadPool extends SelectSockets {

    private static final Logger logger = LogFactory.getLogger(SelectSocketsThreadPool.class);

    private final Map<SocketChannel, TaskThread> tasks = new ConcurrentHashMap<>();
    private final Executor pool;

    public SelectSocketsThreadPool(Executor pool) {
        this.pool = pool;
    }

    @Override
    protected void readableHandler(SelectionKey key) {
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
            } catch (RuntimeException | IOException e) {
                logger.error("Error ! try to close channel : {}", e.getMessage(), e);
                Run.ignoreException(logger, () -> {// close channel
                    closeChannel(key);
                });
            }
        });
    }

    /**
     * Create new Thread to run
     *
     * @param channel socket
     * @param lock    input stream lock
     * @throws IOException error
     */
    protected void readChannel(SocketChannel channel, AwaitRead lock) throws IOException {
        throw new UnsupportedOperationException("暂未实现");
    }

    private static SocketChannel key(SocketChannel channel) {
        return channel;
    }

}