package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.ListData;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.util.NotNull;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class ListOperation extends AbstractOperation {

    // 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
    public void lset(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<ListData> opt = getList(request);
        if (opt.isPresent()) {
            String index = args[1].getByteArray2string();
            try {
                opt.get().getData().set(Integer.parseInt(index), args[2].getByteArray());
                RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
            } catch (ArrayIndexOutOfBoundsException e) {
                RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "Array Index Out Of Bounds");
            }
        } else {
            RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "NO LIST");
        }
    }

    //RPUSH key value [value ...]
    public void rpush(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(1);
        ListData list = this.getOrCreateList(request);
        ExpectRedisRequest[] args = request.getArgs();
        for (int i = 1; i < args.length; i++) {
            list.rightAdd(args[i].getByteArray());
        }
        RedisOutputProtocol.writer(request.getOutputStream(), list.getData().size());
    }

    //RPOP key
    public void rpop(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<ListData> opt = this.getList(request);
        if (opt.isPresent()) {
            try {
                byte[] rm = opt.get().rightPop();
                RedisOutputProtocol.writer(request.getOutputStream(), rm);
                return;
            } catch (IndexOutOfBoundsException i) {// ignore
            }
        }
        RedisOutputProtocol.writerNull(request.getOutputStream());
    }

    //LINDEX key index
    public void lindex(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<ListData> opt = getList(request);
        if (opt.isPresent()) {
            int index = Integer.parseInt(args[1].getByteArray2string());
            List<byte[]> list = opt.get().getData();
            if (index < 0) { // TODO:: 如果被删除 !!!
                index = list.size() + index;
            }
            byte[] bt = list.get(index);
            RedisOutputProtocol.writer(request.getOutputStream(), bt);
        } else {
            RedisOutputProtocol.writerNull(request.getOutputStream());
        }
    }

    private Optional<ListData> getList(RedisRequest request) {
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = new HashKey(args[0].getByteArray());
        RedisDatabase db = request.getDatabase();
        return db.get(key, ListData.class);
    }

    @NotNull
    private ListData getOrCreateList(RedisRequest request) {
        HashKey key = new HashKey(request.getArgs()[0].getByteArray());
        Optional<ListData> opt = this.getList(request);
        if (!opt.isPresent()) {
            ListData d = new ListData();
            ListData origin = request.getDatabase().putIfAbsent(key, d);
            opt = Optional.of(origin == null ? d : origin);
        }
        return opt.get();
    }
}
