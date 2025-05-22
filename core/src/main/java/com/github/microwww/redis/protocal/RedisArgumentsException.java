package com.github.microwww.redis.protocal;

import com.github.microwww.redis.util.Assert;

public class RedisArgumentsException extends RuntimeException {
    public RedisArgumentsException(String message) {
        this(message, null);
    }

    public RedisArgumentsException(String message, Throwable cause) {
        super(message, cause);
        Assert.isNotNull(message, "Not null");
        if (message.contains("\r") || message.contains("\n")) {
            throw new RuntimeException("Not contains \r\n ");
        }
    }
}
