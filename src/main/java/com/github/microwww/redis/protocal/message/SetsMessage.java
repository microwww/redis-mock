package com.github.microwww.redis.protocal.message;

public class SetsMessage extends AbstractCollectionMessage {
    public SetsMessage(Type prefix, RedisMessage[] bytes) {
        super(prefix, bytes);
    }
}
