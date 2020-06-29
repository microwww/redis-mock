package com.github.microwww.redis.database;

import redis.clients.util.SafeEncoder;

import java.io.Serializable;
import java.math.BigInteger;

public final class HashKey implements Serializable {
    private static final long serialVersionUID = 0;

    public final String hash;

    public HashKey(byte[] key) {
        if (key.length == 0) {
            this.hash = "";
        } else {
            this.hash = new BigInteger(key).toString(16);
        }
    }

    public byte[] getKey() {
        if (hash.length() == 0) {
            return new byte[0];
        }
        return new BigInteger(hash, 16).toByteArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HashKey keyData = (HashKey) o;
        return hash.equals(keyData.hash);
    }

    @Override
    public int hashCode() {
        return hash.hashCode();
    }

    @Override
    public String toString() {
        return "HashKey: " + SafeEncoder.encode(this.getKey());
    }
}
