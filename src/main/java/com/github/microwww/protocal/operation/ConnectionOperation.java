package com.github.microwww.protocal.operation;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;
import com.github.microwww.util.Assert;
import redis.clients.jedis.Protocol;
import redis.clients.util.RedisOutputStream;

import java.io.IOException;

public class ConnectionOperation extends AbstractOperation {

    public void ping(RedisRequest request) throws IOException {
        Assert.isTrue(request.getArgs().length == 0, "Not need other arguments");
        RedisOutputStream out = request.getOutputStream();
        RedisOutputProtocol.writer(out, Protocol.Keyword.PONG.name());
    }

    public void select(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 1, "Must only one argument");
        int index = Integer.parseInt(new String(args[0].getByteArray()));
        int db = request.getServer().getSchema().getSize();
        if (index >= db || index < 0) {
            RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "DB index is out of range");
        } else {
            request.getSessions().setDatabase(index);
            RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
        }
    }
}
