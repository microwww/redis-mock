package com.github.microwww.redis.protocal.message;

public class BytesMessage extends RedisMessage {
    public BytesMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
    }
}
