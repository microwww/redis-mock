package com.github.microwww.protocal.operation;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.database.ByteData;
import com.github.microwww.database.HashKey;
import com.github.microwww.database.RedisDatabase;
import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;
import com.github.microwww.util.Assert;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.util.Optional;

public class StringOperation extends AbstractOperation {

    public void set(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 2, "Must has tow arguments");
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        db.put(key, args[1].getByteArray());
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    public void get(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 1, "Must only one argument");
        HashKey key = new HashKey(args[0].getByteArray());
        RedisDatabase db = request.getDatabase();
        Optional<ByteData> val = db.get(key, ByteData.class);
        if (val.isPresent()) {
            RedisOutputProtocol.writer(request.getOutputStream(), val.get().getData());
        } else {
            RedisOutputProtocol.writerNull(request.getOutputStream());
        }
    }
}
