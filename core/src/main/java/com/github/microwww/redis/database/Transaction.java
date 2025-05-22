package com.github.microwww.redis.database;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.Protocol;
import com.github.microwww.redis.protocal.operation.TransactionOperation;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class Transaction implements Closeable {
    public static final String SESSION_NAME = "session." + Transaction.class.getName();
    private final List<RedisRequest> requests;
    private final Map<HashKey, TransactionOperation.DV> watches = new HashMap<>();
    private boolean enable = false;

    public Transaction() {
        this.requests = new ArrayList<>();
    }

    public boolean isEnable() {
        return enable;
    }

    public void start() {
        enable = true;
    }

    public void exec(RedisRequest request) throws IOException {
        Schema schema = request.getServer().getSchema();
        String cmd = request.getCommand();
        if ("multi".equalsIgnoreCase(cmd)) {
            requests.add(request);
            request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
        } else if ("exec".equalsIgnoreCase(cmd)) {
            schema.run(request);// same Thread run
        } else if ("discard".equalsIgnoreCase(cmd)) {
            schema.run(request);// same Thread run
        } else if ("watch".equalsIgnoreCase(cmd)) {
            request.getOutputProtocol().writerError(RedisOutputProtocol.Level.ERR, "WATCH inside MULTI is not allowed");
        } else {
            requests.add(request);
            request.getOutputProtocol().writer(Protocol.Keyword.QUEUED.raw);
        }
    }

    public static Transaction getTransaction(RedisRequest request) {
        Transaction tx = (Transaction) request.getContext().getSessions()
                .computeIfAbsent(SESSION_NAME, k -> new Transaction());
        return tx;
    }

    public static Optional<Transaction> ifTransaction(ChannelContext context) {
        Object tx = context.getSessions().get(SESSION_NAME);
        return Optional.ofNullable((Transaction) tx);
    }

    public List<RedisRequest> getRequests() {
        return Collections.unmodifiableList(requests);
    }

    public Map<HashKey, TransactionOperation.DV> getWatches() {
        return Collections.unmodifiableMap(watches);
    }

    public void putWatch(HashKey hash, TransactionOperation.DV value) {
        this.watches.put(hash, value);
    }

    public void clearWatches() {
        watches.clear();
    }

    @Override
    public void close() {
        enable = false;
        try {
            requests.clear();
        } finally {
            watches.clear();
        }
    }
}
