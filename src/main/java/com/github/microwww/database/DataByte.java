package com.github.microwww.database;

import com.github.microwww.util.Assert;

public class DataByte extends AbstractValueData<byte[]> {

    public DataByte(byte[] data, int exp) {
        Assert.isNotNull(data, "byte[] not null");
        this.data = data;
        this.expire = exp;
    }
}
