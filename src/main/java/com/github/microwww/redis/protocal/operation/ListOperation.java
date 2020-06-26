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
import java.util.Arrays;
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
                Optional<byte[]> rm = opt.get().rightPop();
                RedisOutputProtocol.writer(request.getOutputStream(), rm.orElse(null));
                return;
            } catch (IndexOutOfBoundsException i) {// ignore
            }
        }
        RedisOutputProtocol.writerNull(request.getOutputStream());
    }

    //BLPOP
    //BRPOP
    //BRPOPLPUSH
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

    //LINSERT
    public void linsert(RedisRequest request) throws IOException {
        request.expectArgumentsCount(4);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<ListData> opt = getList(request);
        if (opt.isPresent()) {
            boolean before = false;
            HashKey hk = args[1].byteArray2hashKey();
            String key = args[2].getByteArray2string();
            if (key.equalsIgnoreCase("before")) {
                before = true;
            }
            byte[] pivot = args[3].getByteArray();
            byte[] val = args[4].getByteArray();
            boolean insert = opt.get().findAndOffsetInsert(pivot, before ? 0 : 1, val);
            int len = -1;
            if (insert) {
                len = opt.get().getData().size();
            }
            RedisOutputProtocol.writer(request.getOutputStream(), len);
        } else {
            RedisOutputProtocol.writer(request.getOutputStream(), 0);
        }
    }

    //LLEN
    public void llen(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<ListData> opt = getList(request);
        int size = opt.map(e -> e.getData().size()).orElse(0);
        RedisOutputProtocol.writer(request.getOutputStream(), size);
    }

    //LPOP
    public void lpop(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<ListData> opt = getList(request);
        byte[] data = null;
        if (opt.isPresent()) {
            Optional<byte[]> bytes = opt.get().leftPop();
            data = bytes.orElse(null);
        }
        RedisOutputProtocol.writer(request.getOutputStream(), data);
    }

    //LPUSH
    public void lpush(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ListData data = this.getOrCreateList(request);
        ExpectRedisRequest[] args = request.getArgs();
        byte[][] bytes = Arrays.stream(args, 1, args.length)
                .map(ExpectRedisRequest::getByteArray)
                .toArray(byte[][]::new);
        data.leftAdd(bytes);
        RedisOutputProtocol.writer(request.getOutputStream(), data.getData().size());
    }

    //LPUSHX
    public void lpushx(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        Optional<ListData> opt = this.getList(request);
        if (opt.isPresent()) {
            ExpectRedisRequest[] args = request.getArgs();
            byte[][] bytes = Arrays.stream(args, 1, args.length)
                    .map(ExpectRedisRequest::getByteArray)
                    .toArray(byte[][]::new);
            opt.get().leftAdd(bytes);
        }
        RedisOutputProtocol.writer(request.getOutputStream(), opt.get().getData().size());
    }

    //LRANGE
    public void lrange(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        Optional<ListData> opt = this.getList(request);
        byte[][] range = new byte[0][];
        if (opt.isPresent()) {
            ExpectRedisRequest[] args = request.getArgs();
            range = opt.get().range(args[1].byteArray2int(), args[2].byteArray2int());
        }
        RedisOutputProtocol.writerMulti(request.getOutputStream(), range);
    }

    //LREM
    public void lrem(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        Optional<ListData> opt = this.getList(request);
        int len = 0;
        if (opt.isPresent()) {
            ExpectRedisRequest[] args = request.getArgs();
            len = opt.get().remove(args[1].byteArray2int(), args[2].getByteArray());
        }
        RedisOutputProtocol.writer(request.getOutputStream(), len);
    }

    //LSET
    //LTRIM
    public void ltrim(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        Optional<ListData> opt = getList(request);
        ExpectRedisRequest[] args = request.getArgs();
        opt.ifPresent(e -> {
            e.trim(args[0].byteArray2int(), args[1].byteArray2int());
        });
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    //RPOP
    //RPOPLPUSH
    public void rpoplpush(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        Optional<ListData> opt = this.getList(request);
        byte[] data = null;
        if (opt.isPresent()) {
            ListData source = opt.get();
            ListData destination = this.getOrCreateList(request, 1);
            Optional<byte[]> bytes = source.rightPop();
            if (bytes.isPresent()) {
                data = bytes.get();
                destination.leftAdd(data);
            }
        }
        RedisOutputProtocol.writer(request.getOutputStream(), data);
    }

    //RPUSH
    //RPUSHX
    public void rpushx(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(2);
        Optional<ListData> opt = this.getList(request);
        ExpectRedisRequest[] args = request.getArgs();
        if (opt.isPresent()) {
            byte[] val = args[1].getByteArray();
            opt.get().rightAdd(val);
        }
        RedisOutputProtocol.writer(request.getOutputStream(), opt.map(e -> e.getData().size()).orElse(0));
    }

    private Optional<ListData> getList(RedisRequest request) {
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = new HashKey(args[0].getByteArray());
        RedisDatabase db = request.getDatabase();
        return db.get(key, ListData.class);
    }

    @NotNull
    private ListData getOrCreateList(RedisRequest request) {
        return this.getOrCreateList(request, 0);
    }

    @NotNull
    private ListData getOrCreateList(RedisRequest request, int index) {
        HashKey key = new HashKey(request.getArgs()[index].getByteArray());
        Optional<ListData> opt = this.getList(request);
        if (!opt.isPresent()) {
            ListData d = new ListData();
            ListData origin = request.getDatabase().putIfAbsent(key, d);
            opt = Optional.of(origin == null ? d : origin);
        }
        return opt.get();
    }
}
