package com.github.microwww.redis.protocal.message;

import com.github.microwww.redis.util.SafeEncoder;

public class StringMessage extends RedisMessage {
    private final String value;

    public StringMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
        this.value = SafeEncoder.encode(bytes);
    }

    public String getValue() {
        return value;
    }
}
