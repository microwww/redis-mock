package com.github.microwww.redis.protocal.message;

public class ErrorMessage extends StringMessage {
    public ErrorMessage(Type prefix, byte[] bytes) {
        super(prefix, bytes);
    }
}
