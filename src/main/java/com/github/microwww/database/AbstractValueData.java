package com.github.microwww.database;

public abstract class AbstractValueData<T> {
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
     * @param expire
     */
    public void setSecondsExpire(int expire) {
        if (expire <= 0) {
            expire = 0;
        }
        this.expire = System.currentTimeMillis() + expire * 1000;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public boolean isExpired() {
        if (expire < 0) {
            return false;
        }
        return expire < System.currentTimeMillis();
    }
}
