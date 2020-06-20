package com.github.microwww.database;

import com.github.microwww.util.Assert;

import java.util.Comparator;

public class DataByte extends AbstractValueData<byte[]> {

    public DataByte(byte[] data, int exp) {
        Assert.isNotNull(data, "byte[] not null");
        this.data = data;
        this.expire = exp;
    }

    public static final Comparator<byte[]> COMPARATOR = (o1, o2) -> {
        int length = Math.min(o1.length, o2.length);
        for (int i = 0; i < length; i++) {
            if (o1[i] != o2[i]) {
                return o1[i] - o2[i];
            }
        }
        return o1.length - o2.length;
    };
}
