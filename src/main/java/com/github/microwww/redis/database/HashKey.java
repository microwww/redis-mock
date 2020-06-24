package com.github.microwww.redis.database;

import java.io.Serializable;
import java.math.BigInteger;

public final class HashKey implements Serializable {
    private static final long serialVersionUID = 0;

    public final String hash;

    public HashKey(byte[] key) {
        this.hash = new BigInteger(key).toString(16);
    }

    public byte[] getKey() {
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
}
