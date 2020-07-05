package com.github.microwww.redis.database;

import java.util.BitSet;

public class BitArray {

    private final BitSet data;

    public BitArray(byte[] data) {
        this.data = BitSet.valueOf(data);
    }

    public byte[] toArray() {
        return data.toByteArray();
    }

    public int bitLength() {
        return data.size();
    }

    public int count(boolean one, int from, int includeTo) {
        int count = 0;
        if (bitLength() < 0) {
            return 0;
        }
        int len = this.bitLength();
        while (from < 0) {
            from = len + from;
        }
        if (includeTo < 0) {
            includeTo = len + includeTo;
        }
        for (int i = from; i < len; i++) {
            if (i > includeTo) {
                break;
            }
            boolean o = get(i);
            if (o == one) {
                count++;
            }
        }
        return count;
    }

    public boolean get(int position) {
        return this.data.get(position);
    }

    /**
     * set 1
     *
     * @param position pos
     * @return this
     */
    public BitArray set(int position) {
        this.data.set(position);
        return this;
    }

    public BitArray clean(int position) {
        this.data.clear(position);
        return this;
    }

    @Override
    public String toString() {
        return this.data.toString();
    }
}
