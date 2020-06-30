package com.github.microwww.redis.database;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class Bytes implements Serializable, Comparable {
    private static final long serialVersionUID = 0;

    private final byte[] bytes;

    public Bytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bytes bytes1 = (Bytes) o;
        return Arrays.equals(bytes, bytes1.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof Bytes)) {
            throw new IllegalArgumentException("Un compare object !");
        }
        Bytes o2 = (Bytes) o;
        return ByteData.COMPARATOR.compare(this.bytes, o2.bytes);
    }
}
