package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import redis.clients.jedis.Protocol;

import java.io.IOException;

public class ServerOperation extends AbstractOperation {

    //BGREWRITEAOF
    //BGSAVE
    //CLIENT GETNAME
    //CLIENT KILL
    //CLIENT LIST
    //CLIENT SETNAME
    //CONFIG GET
    //CONFIG RESETSTAT
    //CONFIG REWRITE
    //CONFIG SET
    //DBSIZE
    public void dbsize(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        int size = request.getServer().getSchema().getSize();
        RedisOutputProtocol.writer(request.getOutputStream(), size);
    }

    //DEBUG OBJECT
    //DEBUG SEGFAULT
    //FLUSHALL
    public void flushall(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        Schema db = request.getServer().getSchema();
        db.clearDatabase();
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
    }

    //FLUSHDB
    public void flushdb(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        RedisDatabase db = request.getDatabase();
        db.clear();
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
    }

    //INFO
    //LASTSAVE
    //MONITOR
    //PSYNC
    //SAVE
    //SHUTDOWN
    //SLAVEOF
    //SLOWLOG
    //SYNC
    //TIME
    public void time(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        long time = System.currentTimeMillis();
        long seconds = time / 1_000;
        long micro = time % 1_000;
        RedisOutputProtocol.writerMulti(request.getOutputStream(), (seconds + "").getBytes(), (micro + "000").getBytes());
    }

}
