package com.github.microwww.redis.protocal.message;

public class DoubleMessage extends RedisMessage {
    final private double value;

    public DoubleMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
        String v = new String(bytes);
        if ("inf".equalsIgnoreCase(v)) {
            this.value = Double.POSITIVE_INFINITY;
        } else if ("-inf".equalsIgnoreCase(v)) {
            this.value = Double.NEGATIVE_INFINITY;
        } else this.value = Double.parseDouble(v);
    }

    public double getValue() {
        return value;
    }

    public int toInt() {
        return (int) value;
    }
}
