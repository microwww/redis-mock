package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.SetData;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;

import java.io.IOException;
import java.util.*;

public class SetOperation extends AbstractOperation {

    //SADD
    public void zadd(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();
        SetData ss = request.getDatabase().getOrCreate(hk, SetData::new);

        byte[][] ms = new byte[args.length - 1][];
        for (int i = 0; i < ms.length; i++) {
            ms[i] = args[i + 1].getByteArray();
        }
        int count = ss.add(ms);
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //SCARD
    public void scard(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();
        Optional<SetData> ss = request.getDatabase().get(hk, SetData.class);

        int size = ss.map(e -> e.getData().size()).orElse(0);
        RedisOutputProtocol.writer(request.getOutputStream(), size);
    }

    //SDIFF
    public void sdiff(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();
        Optional<SetData> ss = request.getDatabase().get(hk, SetData.class);
        HashKey[] ms = new HashKey[args.length];
        for (int i = 0; i < ms.length; i++) {
            ms[i] = args[i].byteArray2hashKey();
        }

        Set<Bytes> set = ss.map(e -> {
            Set<Bytes> diff = e.diff(request.getDatabase(), ms, 1);
            return diff;
        }).orElse(Collections.EMPTY_SET);
        byte[][] res = set.stream().map(Bytes::getBytes).toArray(byte[][]::new);

        RedisOutputProtocol.writerMulti(request.getOutputStream(), res);
    }

    //SDIFFSTORE
    public void sdiffstore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey[] ms = new HashKey[args.length];
        for (int i = 0; i < ms.length; i++) {
            ms[i] = args[i].byteArray2hashKey();
        }

        Optional<SetData> ss = request.getDatabase().get(ms[1], SetData.class);
        Set<Bytes> set = ss.map(e -> {
            Set<Bytes> diff = e.diffStore(request.getDatabase(), ms);
            return diff;
        }).orElse(Collections.EMPTY_SET);
        byte[][] res = set.stream().map(Bytes::getBytes).toArray(byte[][]::new);

        RedisOutputProtocol.writerMulti(request.getOutputStream(), res);
    }

    //SINTER
    public void sinter(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();
        Optional<SetData> ss = request.getDatabase().get(hk, SetData.class);
        HashKey[] ms = new HashKey[args.length];
        for (int i = 0; i < ms.length; i++) {
            ms[i] = args[i].byteArray2hashKey();
        }

        Set<Bytes> set = ss.map(e -> {
            Set<Bytes> diff = e.inter(request.getDatabase(), ms, 1);
            return diff;
        }).orElse(Collections.EMPTY_SET);
        byte[][] res = set.stream().map(Bytes::getBytes).toArray(byte[][]::new);

        RedisOutputProtocol.writerMulti(request.getOutputStream(), res);
    }

    //SINTERSTORE
    public void sinterstore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();
        Optional<SetData> ss = request.getDatabase().get(hk, SetData.class);
        HashKey[] ms = new HashKey[args.length];
        for (int i = 0; i < ms.length; i++) {
            ms[i] = args[i].byteArray2hashKey();
        }

        Set<Bytes> set = ss.map(e -> {
            Set<Bytes> diff = e.interStore(request.getDatabase(), ms);
            return diff;
        }).orElse(Collections.EMPTY_SET);
        byte[][] res = set.stream().map(Bytes::getBytes).toArray(byte[][]::new);

        RedisOutputProtocol.writerMulti(request.getOutputStream(), res);
    }

    //SISMEMBER
    public void sismember(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();

        Optional<SetData> data = request.getDatabase().get(hk, SetData.class);
        Bytes bts = new Bytes(args[1].getByteArray());
        boolean exist = data.map(e -> e.getData().contains(bts)).orElse(false);

        RedisOutputProtocol.writer(request.getOutputStream(), exist ? 1 : 0);
    }

    //SMEMBERS
    public void smembers(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();

        Optional<SetData> data = request.getDatabase().get(hk, SetData.class);
        byte[][] bytes = new byte[0][];
        if (data.isPresent()) {
            bytes = data.get().getData()
                    .stream()
                    .map(Bytes::getBytes)
                    .toArray(byte[][]::new);
        }

        RedisOutputProtocol.writerMulti(request.getOutputStream(), bytes);
    }

    //SMOVE
    public void smove(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey source = args[0].byteArray2hashKey();
        HashKey dest = args[1].byteArray2hashKey();
        Bytes member = args[2].toBytes();

        Optional<SetData> data = request.getDatabase().get(source, SetData.class);
        boolean move = false;
        if (data.isPresent()) {
            move = data.get().move(request.getDatabase(), dest, member);
        }

        RedisOutputProtocol.writer(request.getOutputStream(), move ? 1 : 0);
    }

    //SPOP
    public void spop(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey source = args[0].byteArray2hashKey();

        Optional<SetData> data = request.getDatabase().get(source, SetData.class);
        Optional<Bytes> bytes = data.flatMap(SetData::pop);

        RedisOutputProtocol.writer(request.getOutputStream(), bytes.isPresent() ? bytes.get().getBytes() : null);
    }

    //SRANDMEMBER
    public void srandmember(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey source = args[0].byteArray2hashKey();
        Optional<SetData> data = request.getDatabase().get(source, SetData.class);

        List<Bytes> res = data.map(e -> {
            int count = 1;
            if (args.length > 1) {
                count = args[1].byteArray2int();
            }
            if (count == 0) {
            } else if (count < 0) {
                return e.random(0 - count);
            } else {
                return e.exchange(count);
            }
            return new ArrayList<Bytes>();
        }).orElse(Collections.emptyList());

        byte[][] bytes = res.stream().map(Bytes::getBytes).toArray(byte[][]::new);
        RedisOutputProtocol.writerMulti(request.getOutputStream(), bytes);
    }

    //SREM
    public void srem(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);

        ExpectRedisRequest[] args = request.getArgs();
        HashKey source = args[0].byteArray2hashKey();
        Optional<SetData> data = request.getDatabase().get(source, SetData.class);

        Bytes[] bts = new Bytes[args.length - 1];
        for (int i = 0; i < bts.length; i++) {
            bts[i] = args[i + 1].toBytes();
        }

        int count = data.map(e -> {//
            return e.remove(bts);
        }).orElse(0);

        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //SUNION
    //SUNIONSTORE
    //SSCAN
}
