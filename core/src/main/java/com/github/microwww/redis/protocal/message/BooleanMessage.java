package com.github.microwww.redis.protocal.message;

public class BooleanMessage extends RedisMessage {
    final private boolean value;

    public BooleanMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
        String v = new String(bytes);
        switch (v) {
            case "t":
                value = true;
                break;
            case "f":
                value = false;
                break;
            default:
                throw new IllegalArgumentException("Boolean only #t/#f");
        }
    }

    public boolean isValue() {
        return value;
    }
}
