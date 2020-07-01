package com.github.microwww.redis.database;

import redis.clients.util.SafeEncoder;

import java.io.Serializable;
import java.math.BigInteger;

public final class HashKey extends Bytes {
    private static final long serialVersionUID = 0;

    public HashKey(byte[] bytes) {
        super(bytes);
    }

    public byte[] getKey() {
        return super.getBytes();
    }

    @Override
    public String toString() {
        return "HashKey: " + SafeEncoder.encode(this.getKey());
    }
}
