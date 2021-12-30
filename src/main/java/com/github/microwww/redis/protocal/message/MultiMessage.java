package com.github.microwww.redis.protocal.message;

public class MultiMessage extends AbstractCollectionMessage {
    public MultiMessage(Type prefix, RedisMessage[] bytes) {
        super(prefix, bytes);
    }
}
