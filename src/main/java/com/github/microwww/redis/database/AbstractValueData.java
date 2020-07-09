package com.github.microwww.redis.database;

import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractValueData<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    protected final AtomicLong version = new AtomicLong(0);
    public static final int NEVER_EXPIRE = -1;

    long expire = NEVER_EXPIRE;
    T data;

    public long getExpire() {
        return expire;
    }

    public void setExpire(long expire) {
        this.expire = expire;
    }

    /**
     * 如果是负数则表示过期
     *
     * @param expire seconds-expire
     */
    public void setSecondsExpire(int expire) {
        if (expire <= 0) {
            expire = 0;
        }
        this.expire = System.currentTimeMillis() + expire * 1000;
    }

    @NotNull
    public T getData() {
        return data;
    }

    protected void setData(@NotNull T data) {
        Assert.isNotNull(data, "Not null");
        version.getAndIncrement();
        this.data = data;
    }

    public String getType() {
        return "string";
    }

    public boolean isExpired() {
        if (expire < 0) {
            return false;
        }
        return expire <= System.currentTimeMillis();
    }

    public AtomicLong getVersion() {
        return version;
    }
}
