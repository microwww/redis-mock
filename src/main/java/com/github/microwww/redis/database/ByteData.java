package com.github.microwww.redis.database;

import com.github.microwww.redis.util.Assert;

import java.util.Comparator;

public class ByteData extends AbstractValueData<byte[]> {

    public ByteData(byte[] data, long exp) {
        Assert.isNotNull(data, "byte[] not null");
        this.data = data;
        this.expire = exp;
    }

    public static final Comparator<byte[]> COMPARATOR = (o1, o2) -> {
        int length = Math.min(o1.length, o2.length);
        for (int i = 0; i < length; i++) {
            int cm = Byte.compare(o1[i], o2[i]);
            if (cm != 0) {
                return cm;
            }
        }
        return o1.length - o2.length;
    };
}
