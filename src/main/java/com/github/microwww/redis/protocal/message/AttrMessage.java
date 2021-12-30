package com.github.microwww.redis.protocal.message;

public class AttrMessage extends MapMessage {
    public AttrMessage(Type prefix, RedisMessage[] bytes) {
        super(prefix, bytes);
    }
}
