package com.github.microwww.redis.protocal.message;

import java.util.Arrays;

public class AbstractCollectionMessage extends RedisMessage {
    private final RedisMessage[] messages;

    public AbstractCollectionMessage(Type prefix, RedisMessage[] bytes) {
        super(prefix, null);
        messages = bytes;
    }

    @Override
    public byte[] getBytes() {
        return this.messages.length > 0 ? this.messages[0].getBytes() : null;
    }

    public RedisMessage[] getRedisMessages() {
        return messages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AbstractCollectionMessage that = (AbstractCollectionMessage) o;
        return Arrays.equals(messages, that.messages);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(messages);
        return result;
    }

    @Override
    public String toString() {
        return Arrays.toString(messages);
    }
}
