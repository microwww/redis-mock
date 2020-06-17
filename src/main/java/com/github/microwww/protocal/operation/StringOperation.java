package com.github.microwww.protocal.operation;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.database.*;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;
import com.github.microwww.util.Assert;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.util.Optional;

public class StringOperation {

    public void select(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 1, "Must only one argument");
        int index = Integer.parseInt(new String(args[0].getByteArray()));
        int db = request.getServer().getSchema().getSize();
        if (index >= db || index < 0) {
            RedisOutputProtocol.writerError(request.getOutputStream(), "ERR", "DB index is out of range");
        } else {
            request.getSessions().setDatabase(index);
            RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
        }
    }

    public void set(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 2, "Must has tow arguments");
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        db.put(key, args[1].getByteArray());
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    public void expire(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 2, "Must has tow arguments");
        HashKey key = new HashKey(args[0].getByteArray());
        int exp = Integer.parseInt(new String(args[1].getByteArray()));
        RedisDatabase db = request.getDatabase();
        Optional<AbstractValueData<?>> val = db.get(key);
        val.ifPresent(e -> {//
            e.setSecondsExpire(exp);
        });
        RedisOutputProtocol.writer(request.getOutputStream(), 1);
    }

    public void get(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 1, "Must only one argument");
        HashKey key = new HashKey(args[0].getByteArray());
        RedisDatabase db = request.getDatabase();
        Optional<DataByte> val = db.get(key, DataByte.class);
        if (val.isPresent()) {
            RedisOutputProtocol.writer(request.getOutputStream(), val.get().getData());
        } else {
            RedisOutputProtocol.writerNull(request.getOutputStream());
        }
    }
}