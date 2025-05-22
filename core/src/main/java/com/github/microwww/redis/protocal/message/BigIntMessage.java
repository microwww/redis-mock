package com.github.microwww.redis.protocal.message;

public class BigIntMessage extends RedisMessage {
    public BigIntMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
    }
}
