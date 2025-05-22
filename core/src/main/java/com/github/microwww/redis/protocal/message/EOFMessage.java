package com.github.microwww.redis.protocal.message;

public class EOFMessage extends RedisMessage {
    public EOFMessage(Type prefix) {
        super(prefix, null);
    }
}
