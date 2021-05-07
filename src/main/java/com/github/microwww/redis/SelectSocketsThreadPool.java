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
            Exception ex = null;
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
                try {
                    tasks.put(k, thread); // 原先已经停止, 开启新的读取
                    thread.scheduling((lock) -> {
                        readChannel(channel, lock);
                    });
                } finally {
                    tasks.remove(k, thread);
                }
            } catch (RuntimeException e) {
                ex = e;
                logger.error("invoke channel error !", e);
            } catch (IOException e) {
                ex = e;
                // We have no API to detect if the channel is closed. so if catch an IOException, The client closed the connection voluntarily !
                logger.debug("client channel is close ! <IOException>", e);
            }
            if (ex != null) {
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