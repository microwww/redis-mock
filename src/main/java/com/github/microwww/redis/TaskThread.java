package com.github.microwww.redis;

import com.github.microwww.redis.util.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TaskThread {
    private static final int EXIT = -1;
    private final Lock lock = new ReentrantLock();
    private final AtomicInteger status = new AtomicInteger(1);
    private AwaitRead awaitRead;

    /**
     * invoke read.run()
     *
     * @param read
     * @throws IOException
     */
    public void scheduling(Reading read) throws IOException {
        awaitRead = new AwaitRead(Thread.currentThread());
        lock.lock();
        try {
            while (true) {
                synchronized (status) {
                    if (status.get() == 0 || status.get() == EXIT) {
                        status.set(EXIT);
                        break;
                    }
                    status.set(0);
                }
                read.read(awaitRead);
            }
        } catch (Exception ex) { // 出错暂不处理
            throw ex;
        } finally {
            lock.unlock();
        }
    }

    private void inLock(SocketChannel channel) throws IOException {
        ByteBuffer bf = ByteBuffer.allocate(1024);
        while (true) {
            int read = channel.read(bf);
            if (read == -1) { // 远程正常关闭连接
                throw new IOException("Remote close");
            }
            if (read <= 0) {
                break;
            }
            throw new UnsupportedOperationException("实现中......");
        }
    }

    /**
     * 线程中任务追加成功 返回 true, 否则返回 false, 追加失败需要新线程处理
     *
     * @return
     */
    public boolean append() {
        synchronized (status) {
            if (lock.tryLock()) {
                try {
                    status.set(EXIT);
                    return false;
                } finally {
                    lock.unlock();
                }
            }
            if (status.get() == EXIT) {
                return false;
            }
            status.incrementAndGet();
            Assert.isNotNull(awaitRead, "await-read is NULL");
            awaitRead.unpark();
            return true;
        }
    }
}