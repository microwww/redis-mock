package com.github.microwww;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TaskThread {
    private static ConcurrentHashMap<String, TaskThread> tasks = new ConcurrentHashMap();
    private Lock lock = new ReentrantLock();
    private AtomicBoolean stop = new AtomicBoolean();
    private final Thread thread;

    public TaskThread(int count, Thread thread) {
        this.thread = thread;
    }

    public static void addTask(SocketChannel channel) throws IOException {
        String k = key(channel);
        TaskThread thread = new TaskThread(1, Thread.currentThread());
        boolean doing = false;
        TaskThread tt = tasks.get(k);
        if (tt != null) {
            doing = tt.append();
        }
        if (doing) {
            return;
        }
        tasks.put(k, thread); // 原先已经停止, 开启新的读取
        thread.reader(channel);
    }

    private void reader(SocketChannel channel) throws IOException {
        while (!stop.get()) {
            lock.lock();
            try {
                if (stop.get()) {
                    break;
                }
                stop.set(true);
                inLock(channel);
            } finally {
                lock.unlock();
            }
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
        }
    }

    /**
     * 线程中任务追加成功 返回 true, 否则返回 false, 追加失败需要新线程处理
     *
     * @return
     */
    public boolean append() {
        if (lock.tryLock()) {
            try {
                stop.set(true);
                return false;
            } finally {
                lock.unlock();
            }
        }
        stop.set(false);
        return true;
    }

    public static String key(SocketChannel channel) throws IOException {
        InetSocketAddress address = (InetSocketAddress) channel.getRemoteAddress();
        return address.getHostName() + ":" + address.getPort();
    }
}