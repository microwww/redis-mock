package com.github.microwww.protocal.operation;

import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;

import java.io.IOException;

public class ServerOperation extends AbstractOperation {

    public void time(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        long time = System.currentTimeMillis();
        long seconds = time / 1_000;
        long micro = time % 1_000;
        RedisOutputProtocol.writerMulti(request.getOutputStream(), seconds + "", micro + "000");
    }

}
