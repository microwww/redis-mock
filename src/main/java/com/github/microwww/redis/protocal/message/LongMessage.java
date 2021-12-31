package com.github.microwww.redis.protocal.message;

import com.github.microwww.redis.util.SafeEncoder;

public class LongMessage extends RedisMessage {
    public LongMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
    }

    public long toLong() {
        return Long.parseLong(SafeEncoder.encode(bytes));
    }
}
