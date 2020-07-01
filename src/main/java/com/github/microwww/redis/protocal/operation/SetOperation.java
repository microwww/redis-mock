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
    public void sadd(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();
        SetData ss = request.getDatabase().getOrCreate(hk, SetData::new);
        Bytes[] ms = Arrays.stream(args, 1, args.length).map(e -> e.toBytes())
                .toArray(Bytes[]::new);

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
        HashKey[] keys = Arrays.stream(args).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);

        byte[][] res = new SetData().diffStore(request.getDatabase(), keys) //
                .stream().map(Bytes::getBytes).toArray(byte[][]::new);

        RedisOutputProtocol.writerMulti(request.getOutputStream(), res);
    }

    //SDIFFSTORE
    public void sdiffstore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey[] keys = Arrays.stream(args, 1, args.length).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        SetData oc = this.getOrCreate(request);
        Set<Bytes> res = oc.diffStore(request.getDatabase(), keys);

        RedisOutputProtocol.writer(request.getOutputStream(), res.size());
    }

    //SINTER
    public void sinter(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();

        HashKey[] keys = Arrays.stream(args).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        Set<Bytes> bytes = new SetData().interStore(request.getDatabase(), keys);
        byte[][] res = bytes.stream().map(Bytes::getBytes).toArray(byte[][]::new);

        RedisOutputProtocol.writerMulti(request.getOutputStream(), res);
    }

    //SINTERSTORE
    public void sinterstore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        SetData oc = this.getOrCreate(request);

        HashKey[] keys = Arrays.stream(args, 1, args.length).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        Set<Bytes> bytes = oc.interStore(request.getDatabase(), keys);

        RedisOutputProtocol.writer(request.getOutputStream(), bytes.size());
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

        RedisOutputProtocol.writer(request.getOutputStream(), bytes.map(Bytes::getBytes).orElse(null));
    }

    //SRANDMEMBER
    public void srandmember(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey source = args[0].byteArray2hashKey();
        Optional<SetData> data = request.getDatabase().get(source, SetData.class);

        List<Bytes> list = Collections.emptyList();
        if (data.isPresent()) {
            int count = 1;
            if (args.length > 1) {
                count = args[1].byteArray2int();
            }
            if (count > 0) {
                list = data.get().exchange(count);
            } else if (count < 0) {
                list = data.get().random(0 - count);
            }
        }

        byte[][] bytes = list.stream().map(Bytes::getBytes).toArray(byte[][]::new);
        RedisOutputProtocol.writerMulti(request.getOutputStream(), bytes);
    }

    //SREM
    public void srem(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);

        ExpectRedisRequest[] args = request.getArgs();
        HashKey source = args[0].byteArray2hashKey();
        Optional<SetData> data = request.getDatabase().get(source, SetData.class);

        int count = 0;
        if (data.isPresent()) {
            Bytes[] bts = Arrays.stream(args, 1, args.length).map(e -> e.toBytes())
                    .toArray(Bytes[]::new);
            count = data.get().removeAll(bts);
        }

        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //SUNION
    public void sunion(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey[] keys = Arrays.stream(args).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        Set<Bytes> union = new SetData().union(request.getDatabase(), keys);

        RedisOutputProtocol.writerMulti(request.getOutputStream(), union.stream().map(Bytes::getBytes).toArray(byte[][]::new));
    }

    //SUNIONSTORE
    public void sunionstore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();

        SetData dest = this.getOrCreate(request);
        HashKey[] keys = Arrays.stream(args, 1, args.length).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        Set<Bytes> union = dest.union(request.getDatabase(), keys);

        RedisOutputProtocol.writer(request.getOutputStream(), union.size());
    }
    //SSCAN

    public SetData getOrCreate(RedisRequest request) {
        ExpectRedisRequest[] args = request.getArgs();
        HashKey source = args[0].byteArray2hashKey();
        return request.getDatabase().getOrCreate(source, SetData::new);
    }
}
