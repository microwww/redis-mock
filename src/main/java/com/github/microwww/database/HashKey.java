package com.github.microwww.database;

import java.io.Serializable;
import java.math.BigInteger;

public final class HashKey implements Serializable {
    private static final long serialVersionUID = 0;

    public final String hash;

    public HashKey(byte[] key) {
        this.hash = new BigInteger(key).toString(16);
    }

    public HashKey(String key) {
        this.hash = key;
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
