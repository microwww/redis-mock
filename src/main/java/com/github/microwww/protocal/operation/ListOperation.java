package com.github.microwww.protocal.operation;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.database.DataList;
import com.github.microwww.database.HashKey;
import com.github.microwww.database.RedisDatabase;
import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;
import com.github.microwww.util.NotNull;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class ListOperation extends AbstractOperation {

    // 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
    public void lset(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<DataList> opt = getList(request);
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
        DataList list = this.getOrCreateList(request);
        ExpectRedisRequest[] args = request.getArgs();
        for (int i = 1; i < args.length; i++) {
            list.getData().add(args[i].getByteArray());
        }
        RedisOutputProtocol.writer(request.getOutputStream(), list.getData().size());
    }

    //RPOP key
    public void rpop(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<DataList> opt = this.getList(request);
        if (opt.isPresent()) {
            List<byte[]> list = opt.get().getData();
            while (true) {
                try {
                    if (!list.isEmpty()) {
                        byte[] rm = list.remove(list.size() - 1);
                        RedisOutputProtocol.writer(request.getOutputStream(), rm);
                        return;
                    }
                } catch (IndexOutOfBoundsException i) {// ignore
                }
            }
        }
        RedisOutputProtocol.writerNull(request.getOutputStream());
    }

    //LINDEX key index
    public void lindex(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<DataList> opt = getList(request);
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

    private Optional<DataList> getList(RedisRequest request) {
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = new HashKey(args[0].getByteArray2string());
        RedisDatabase db = request.getDatabase();
        return db.get(key, DataList.class);
    }

    @NotNull
    private DataList getOrCreateList(RedisRequest request) {
        HashKey key = new HashKey(request.getArgs()[0].getByteArray2string());
        Optional<DataList> opt = this.getList(request);
        if (!opt.isPresent()) {
            DataList d = new DataList();
            DataList origin = request.getDatabase().putIfAbsent(key, d);
            opt = Optional.of(origin == null ? d : origin);
        }
        return opt.get();
    }
}
