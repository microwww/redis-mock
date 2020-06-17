package com.github.microwww.database;

public abstract class AbstractValueData<T> {
    public static final int NEVER_EXPIRE = -1;

    int expire = NEVER_EXPIRE;
    T data;

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
