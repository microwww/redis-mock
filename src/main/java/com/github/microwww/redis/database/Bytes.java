package com.github.microwww.redis.database;

import redis.clients.util.SafeEncoder;

import java.io.Serializable;
import java.util.Arrays;

public class Bytes implements Serializable, Comparable<Bytes> {
    private static final long serialVersionUID = 0;

    private final byte[] bytes;
    public final int length;

    public Bytes(String data) {
        this.bytes = SafeEncoder.encode(data);
        this.length = bytes.length;
    }

    public Bytes(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.length = bytes.length;
    }

    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    public byte[] copyByte(int newLength) {
        return Arrays.copyOf(this.bytes, newLength);
    }

    public int toInt() {
        return Integer.parseInt(this.toString());
    }

    public long toLong() {
        return Long.parseLong(this.toString());
    }

    public static boolean eq(Bytes bytes, byte[] o) {
        if (bytes == null) {
            return o == null;
        }
        return bytes.eq(o);
    }

    public boolean eq(byte[] o) {
        return Arrays.equals(this.bytes, o);
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
    public int compareTo(Bytes o) {
        return ByteData.COMPARATOR.compare(this.bytes, o.bytes);
    }

    @Override
    public String toString() {
        return SafeEncoder.encode(this.bytes);
    }
}
