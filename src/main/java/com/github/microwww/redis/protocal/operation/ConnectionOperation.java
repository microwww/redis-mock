package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.util.Assert;
import redis.clients.jedis.Protocol;
import redis.clients.util.RedisOutputStream;

import java.io.IOException;

public class ConnectionOperation extends AbstractOperation {

    /**
     * do nothing, return OK
     *
     * @param request
     * @throws IOException
     */
    public void auth(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    public void ping(RedisRequest request) throws IOException {
        Assert.isTrue(request.getArgs().length == 0, "Not need other arguments");
        RedisOutputStream out = request.getOutputStream();
        RedisOutputProtocol.writer(out, Protocol.Keyword.PONG.name());
    }

    public void select(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 1, "Must only one argument");
        int index = Integer.parseInt(args[0].getByteArray2string());
        int db = request.getServer().getSchema().getSize();
        if (index >= db || index < 0) {
            RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "DB index is out of range");
        } else {
            request.getSessions().setDatabase(index);
            RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
        }
    }
}
