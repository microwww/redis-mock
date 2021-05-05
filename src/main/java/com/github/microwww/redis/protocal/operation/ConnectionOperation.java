package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.exception.RequestQuitException;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.protocal.jedis.Protocol;

import java.io.IOException;

public class ConnectionOperation extends AbstractOperation {

    //AUTH

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

    //ECHO
    public void echo(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        byte[] echo = request.getArgs()[0].getByteArray();
        RedisOutputProtocol.writer(request.getOutputStream(), echo);
    }

    //PING
    public void ping(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        JedisOutputStream out = request.getOutputStream();
        RedisOutputProtocol.writer(out, "PONG");
    }

    //QUIT
    public void quit(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        JedisOutputStream out = request.getOutputStream();
        RedisOutputProtocol.writer(out, Protocol.Keyword.OK.name());
        out.flush();
        throw new RequestQuitException();
    }

    //SELECT
    public void select(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        ExpectRedisRequest[] args = request.getArgs();
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
