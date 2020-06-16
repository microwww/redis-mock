package com.github.microwww;

import com.github.microwww.util.Assert;

import java.util.concurrent.locks.LockSupport;

public class AwaitRead {
    private final Thread thread;

    public AwaitRead(Thread thread) {
        Assert.isNotNull(thread, "thread argument must be not NULL");
        this.thread = thread;
    }

    public void park() {
        Assert.isTrue(Thread.currentThread().equals(thread), "Block ERROR ! current-thread must equal cache thread");
        LockSupport.park();
    }

    public void unpark() {
        LockSupport.unpark(thread);
    }
}
