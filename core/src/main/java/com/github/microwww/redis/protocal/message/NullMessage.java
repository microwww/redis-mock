package com.github.microwww.redis.protocal.message;

public class NullMessage extends RedisMessage {
    public NullMessage(Type prefix) {
        super(prefix, null);
    }
}
