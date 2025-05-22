package com.github.microwww.redis.protocal.message;

import com.github.microwww.redis.util.SafeEncoder;

import java.util.Arrays;
import java.util.Objects;

public abstract class RedisMessage {
    public final Type type;
    protected final byte[] bytes;

    private AttrMessage attr;

    public RedisMessage(Type type, byte[] bytes) {
        this.type = type;
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    public boolean isEmpty() {
        return bytes == null || bytes.length == 0;
    }

    public RedisMessage[] getRedisMessages() {
        return new RedisMessage[]{this};
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisMessage that = (RedisMessage) o;
        return type == that.type && Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type);
        result = 31 * result + Arrays.hashCode(bytes);
        return result;
    }

    public AttrMessage getAttr() {
        return attr;
    }

    public RedisMessage setAttr(AttrMessage attr) {
        this.attr = attr;
        return this;
    }

    @Override
    public String toString() {
        if (bytes == null) return "NULL";
        return SafeEncoder.encode(bytes);
    }
}
