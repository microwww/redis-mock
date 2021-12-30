package com.github.microwww.redis.protocal.message;

public class LongMessage extends RedisMessage {
    public LongMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
    }
}
