package com.github.microwww.redis.protocal.message;

public class VerbatimMessage extends RedisMessage {
    public VerbatimMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
    }
}
