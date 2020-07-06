package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransactionOperation extends AbstractOperation {

    //DISCARD
    //EXEC
    public void exec(RedisRequest request) throws IOException {
        RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "Must start with MULTI");
    }

    private void exec(RedisRequest request, List<RedisRequest> requests) throws IOException {
        for (RedisRequest r : requests) {
            RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "Not support MULTI : " + r.getCommand());
        }
        request.getOutputStream().flush();
    }

    //MULTI
    public void multi(RedisRequest request) throws IOException {
        List<RedisRequest> requests = new ArrayList<>();
        while (true) {
            Object read = Protocol.read(request.getInputStream());
            ExpectRedisRequest[] param = ExpectRedisRequest.parseRedisData(read);
            RedisRequest rr = new RedisRequest(request.getServer(), request.getChannel(), param);
            String cmd = rr.getCommand();
            if ("exec".equalsIgnoreCase(cmd)) {
                this.exec(request, requests);
                break;
            } else {
                requests.add(rr);
                RedisOutputProtocol.writerBuffer(request.getOutputStream(), Protocol.Keyword.QUEUED.raw);
            }
        }
    }
    //UNWATCH
    //WATCH

}
