package com.github.microwww.redis.script;

import java.io.IOException;

import com.github.microwww.redis.protocal.RedisRequest;

public interface Script {
    void eval(RedisRequest request) throws IOException;
}
