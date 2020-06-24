package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;

import java.io.IOException;

public class ServerOperation extends AbstractOperation {

    public void time(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        long time = System.currentTimeMillis();
        long seconds = time / 1_000;
        long micro = time % 1_000;
        RedisOutputProtocol.writerMulti(request.getOutputStream(), (seconds + "").getBytes(), (micro + "000").getBytes());
    }

}
