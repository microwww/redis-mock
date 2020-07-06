package com.github.microwww.redis.exception;

import java.io.IOException;

public class RequestInterruptedException extends IOException {
    public RequestInterruptedException(String message, InterruptedException cause) {
        super(message, cause);
    }
}
