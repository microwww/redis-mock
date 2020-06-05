package com.github.microwww.database;

import java.util.Arrays;

public class KeyData {
    final byte[] key;

    public KeyData(byte[] key) {
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyData keyData = (KeyData) o;
        return Arrays.equals(key, keyData.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
