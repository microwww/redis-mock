package com.github.microwww.redis.protocal.message;

public class PushMessage extends AbstractCollectionMessage {
    public PushMessage(Type prefix, RedisMessage[] bytes) {
        super(prefix, bytes);
    }
}
