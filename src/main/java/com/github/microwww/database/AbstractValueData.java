package com.github.microwww.database;

public abstract class AbstractValueData<T> {
    int exp = -1;
    T data;

    public int getExp() {
        return exp;
    }

    public void setExp(int exp) {
        this.exp = exp;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
