package com.github.microwww.redis.database;

import redis.clients.util.SafeEncoder;

public final class HashKey extends Bytes {
    private static final long serialVersionUID = 0;

    public HashKey(byte[] bytes) {
        super(bytes);
    }

    @Override
    public String toString() {
        return "HashKey: " + SafeEncoder.encode(this.getBytes());
    }
}
