package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.AbstractValueData;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.protocal.jedis.Protocol;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.StringUtil;

import java.io.IOException;
import java.util.*;

public class TransactionOperation extends AbstractOperation {

    private static final Logger log = LogFactory.getLogger(TransactionOperation.class);
    public static final String MULTI_SESSION_KEY = TransactionOperation.class.getName() + ".MULTI";
    public static final String WATCH_SESSION_KEY = TransactionOperation.class.getName() + ".WATCH";

    //DISCARD
    public void discard(RedisRequest request) throws IOException {
        try {
            RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
        } finally {
            request.getSessions().remove(MULTI_SESSION_KEY);
            request.getSessions().remove(WATCH_SESSION_KEY);
        }
    }

    //EXEC
    public void exec(RedisRequest request) throws IOException {
        try {
            this.tryExec(request);
        } finally {
            request.getSessions().remove(MULTI_SESSION_KEY);
            request.getSessions().remove(WATCH_SESSION_KEY);
        }
    }

    private void tryExec(RedisRequest request) throws IOException {
        List<RedisRequest> rqs = (List<RedisRequest>) request.getSessions().get(MULTI_SESSION_KEY);
        Assert.isNotEmpty(rqs, "Must start with MULTI, But not find command");
        Assert.isTrue("multi".equalsIgnoreCase(rqs.get(0).getCommand()), "Must start with MULTI");
        JedisOutputStream out = request.getOutputStream();

        Map<HashKey, DV> watch = (Map<HashKey, DV>) request.getSessions().get(WATCH_SESSION_KEY);
        if (watch != null) {
            Optional<HashKey> notEqual = watch.keySet().stream().filter((k) -> {// to version null-able
                DV ver = watch.get(k);
                AbstractValueData<?> ov = request.getDatabase().get(k).orElse(null);
                return !ver.eq(ov); // not equal
            }).findAny();
            if (notEqual.isPresent()) {
                RedisOutputProtocol.writerMulti(out, (byte[][]) null);
                return;
            }
        }

        out.write(Protocol.ASTERISK_BYTE);
        out.writeIntCrLf(rqs.size() - 1);

        for (int i = 1; i < rqs.size(); i++) {
            try {
                if ("unwatch".equalsIgnoreCase(request.getCommand())) { // ignore , client 2.9 will error !
                    RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
                    continue;
                }
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
                // 2.0 client not need server send any data, when client send next commend
                // 3.0 client get the server send data and then send next commend
                if (request.getInputStream().available() <= 0) { // TODO: bugger ? always equal '0'
                    request.getOutputStream().flush();
                }

                Object read = request.getInputStream().readRedisData();
                ExpectRedisRequest[] param = ExpectRedisRequest.parseRedisData(read);
                RedisRequest rr = RedisRequest.warp(request, param);
                String cmd = rr.getCommand();
                if ("exec".equalsIgnoreCase(cmd)) {
                    request.getServer().getSchema().submit(rr);// new Thread
                    break;
                } else if ("discard".equalsIgnoreCase(cmd)) {
                    request.getServer().getSchema().submit(rr);// new Thread
                    break;
                } else if ("watch".equalsIgnoreCase(cmd)) {
                    RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "WATCH inside MULTI is not allowed");
                    break;
                } else {
                    rqs.add(rr);
                    RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.QUEUED.raw);
                }
            }
        });
    }

    //UNWATCH
    public void unwatch(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        request.getSessions().remove(WATCH_SESSION_KEY);
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
    }

    //WATCH
    public void watch(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        request.getSessions().putIfAbsent(WATCH_SESSION_KEY, new HashMap<>());
        Map<HashKey, DV> watch = (Map<HashKey, DV>) request.getSessions().get(WATCH_SESSION_KEY);
        for (ExpectRedisRequest arg : request.getArgs()) {
            HashKey hk = arg.byteArray2hashKey();
            Optional<AbstractValueData<?>> val = request.getDatabase().get(hk);
            watch.put(hk, val.map(e -> new DV(e, e.getVersion().get())).orElse(new DV(null, null)));
        }
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
    }

    public static class DV {
        public final AbstractValueData<?> data;
        public final Long version;

        public DV(AbstractValueData<?> data, Long version) {
            this.data = data;
            this.version = version;
        }

        public boolean eq(AbstractValueData<?> dv) {
            if (this.data == dv) {
                if (dv == null) {
                    return true;
                }
                return this.version.equals(dv.getVersion().get());
            }
            return false;
        }
    }
}
