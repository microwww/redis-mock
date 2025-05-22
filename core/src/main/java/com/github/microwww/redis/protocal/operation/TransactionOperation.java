package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.AbstractValueData;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.Transaction;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TransactionOperation extends AbstractOperation {

    private static final Logger log = LogFactory.getLogger(TransactionOperation.class);

    //DISCARD
    public void discard(RedisRequest request) throws IOException {
        try {
            request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
        } finally {
            Transaction.getTransaction(request).close();
        }
    }

    //EXEC
    public void exec(RedisRequest request) throws IOException {
        Transaction transaction = Transaction.getTransaction(request);
        try {
            this.tryExec(transaction, request);
        } finally {
            transaction.close();
        }
    }

    private void tryExec(Transaction transaction, RedisRequest request) throws IOException {
        List<RedisRequest> rqs = transaction.getRequests();
        Assert.isNotEmpty(rqs, "Must start with MULTI, But not find command");
        Assert.isTrue("multi".equalsIgnoreCase(rqs.get(0).getCommand()), "Must start with MULTI");
        JedisOutputStream out = request.getOutputProtocol().getOut();

        Map<HashKey, DV> watch = transaction.getWatches();
        if (watch != null) {
            Optional<HashKey> notEqual = watch.keySet().stream().filter((k) -> {// to version null-able
                DV ver = watch.get(k);
                AbstractValueData<?> ov = request.getDatabase().get(k).orElse(null);
                return !ver.eq(ov); // not equal
            }).findAny();
            if (notEqual.isPresent()) {
                request.getOutputProtocol().writerMulti((byte[][]) null);
                return;
            }
        }

        out.write(Protocol.ASTERISK_BYTE);
        out.writeIntCrLf(rqs.size() - 1);

        for (int i = 1; i < rqs.size(); i++) {
            try {
                if ("unwatch".equalsIgnoreCase(request.getCommand())) { // ignore , client 2.9 will error !
                    request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
                    continue;
                }
                request.getServer().getSchema().run(rqs.get(i)); // same thread !
            } catch (IOException ex) {
                String message = StringUtil.redisErrorMessage(ex);
                log.error("IO error ! {}", message, ex);
                // write error, not flush
                out.write(Protocol.MINUS_BYTE);
                out.writeAsciiCrLf(message);
                // request.getOutputProtocol().writerError(out, RedisOutputProtocol.Level.ERR, message);
            }
        }
    }

    private void exec(RedisRequest request, List<RedisRequest> requests) throws IOException {
        for (RedisRequest r : requests) {
            request.getOutputProtocol().writerError(RedisOutputProtocol.Level.ERR, "Not support MULTI : " + r.getCommand());
        }
        request.getOutputProtocol().flush();
    }

    //MULTI
    public void multi(RedisRequest request) throws IOException {
        Transaction tx = Transaction.getTransaction(request);
        tx.start();
        log.debug("Start one transaction {}", request.getContext().getRemoteHost());
        tx.exec(request);
    }

    //UNWATCH
    public void unwatch(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        Transaction.getTransaction(request).clearWatches();
        request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
    }

    //WATCH
    public void watch(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        Transaction watch = Transaction.getTransaction(request);
        for (RequestParams arg : request.getParams()) {
            HashKey hk = arg.byteArray2hashKey();
            Optional<AbstractValueData<?>> val = request.getDatabase().get(hk);
            watch.putWatch(hk, val.map(e -> new DV(e, e.getVersion().get())).orElse(new DV(null, null)));
        }
        request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
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
