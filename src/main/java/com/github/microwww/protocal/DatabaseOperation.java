package com.github.microwww.protocal;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.database.RedisDatabase;
import com.github.microwww.database.Schema;
import com.github.microwww.util.Assert;
import redis.clients.jedis.Protocol;

import java.io.IOException;

public class DatabaseOperation {

    public void select(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 1, "Must only one argument");
        int index = Integer.valueOf(new String(args[0].getByteArray())).intValue();
        int db = Schema.getDef().getSize();
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
        int index = request.getSessions().getDatabase();
        RedisDatabase db = Schema.getDef().getRedisDatabases(index);
        db.put(args[0].getByteArray(), args[1].getByteArray());
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }
}
