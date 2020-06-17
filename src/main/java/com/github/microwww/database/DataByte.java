package com.github.microwww.database;

public class DataByte extends AbstractValueData<byte[]> {

    public DataByte(byte[] data, int exp) {
        this.data = data;
        this.expire = exp;
    }
}
