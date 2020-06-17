package com.github.microwww.protocal;

import com.github.microwww.util.Assert;
import redis.clients.jedis.Protocol;
import redis.clients.util.RedisOutputStream;

import java.io.IOException;

public class ServerOperation {

    public void ping(RedisRequest request) throws IOException {
        Assert.isTrue(request.getArgs().length == 0, "Not need other arguments");
        RedisOutputStream out = request.getOutputStream();
        RedisOutputProtocol.writer(out, Protocol.Keyword.PONG.name());
        out.flush();
    }
}
