package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.StringUtil;
import redis.clients.jedis.Protocol;
import redis.clients.util.RedisOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransactionOperation extends AbstractOperation {

    private static final Logger log = LogFactory.getLogger(TransactionOperation.class);
    public static final String MULTI_SESSION_KEY = TransactionOperation.class.getName() + ".MULTI";

    //DISCARD
    //EXEC
    public void exec(RedisRequest request) throws IOException {
        List<RedisRequest> rqs = (List<RedisRequest>) request.getSessions().get(MULTI_SESSION_KEY);
        Assert.isNotEmpty(rqs, "exec not find command");
        Assert.isTrue("multi".equalsIgnoreCase(rqs.get(0).getCommand()), "Must start with MULTI");
        RedisOutputStream out = request.getOutputStream();
        out.write(Protocol.ASTERISK_BYTE);
        out.writeIntCrLf(rqs.size() - 1);
        for (int i = 1; i < rqs.size(); i++) {
            try {
                request.getServer().getSchema().run(rqs.get(i)); // same thread !
            } catch (IOException ex) {
                String message = StringUtil.redisErrorMessage(ex);
                log.error("IO error ! {}", message, ex);
                // write error, not flush
                out.write(Protocol.MINUS_BYTE);
                out.writeAsciiCrLf(message);
                // RedisOutputProtocol.writerError(out, RedisOutputProtocol.Level.ERR, message);
            }
        }
    }

    private void exec(RedisRequest request, List<RedisRequest> requests) throws IOException {
        for (RedisRequest r : requests) {
            RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "Not support MULTI : " + r.getCommand());
        }
        request.getOutputStream().flush();
    }

    //MULTI
    public void multi(RedisRequest request) throws IOException {
        request.getSessions().putIfAbsent(MULTI_SESSION_KEY, new ArrayList<>());
        List<RedisRequest> rqs = (List<RedisRequest>) request.getSessions().get(MULTI_SESSION_KEY);
        rqs.add(request);
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
        request.setNext((o) -> {
            while (true) {
                Object read = Protocol.read(request.getInputStream());
                ExpectRedisRequest[] param = ExpectRedisRequest.parseRedisData(read);
                RedisRequest rr = RedisRequest.warp(request, param);
                String cmd = rr.getCommand();
                if ("exec".equalsIgnoreCase(cmd)) {
                    request.getServer().getSchema().submit(rr);// new Thread
                    request.getOutputStream().flush();
                    break;
                } else {
                    rqs.add(rr);
                    RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.QUEUED.raw);
                }
            }
        });
    }
    //UNWATCH
    //WATCH

}
