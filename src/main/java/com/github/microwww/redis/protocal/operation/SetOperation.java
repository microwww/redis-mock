package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.SetData;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

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
    //SINTERSTORE
    //SISMEMBER
    //SMEMBERS
    //SMOVE
    //SPOP
    //SRANDMEMBER
    //SREM
    //SUNION
    //SUNIONSTORE
    //SSCAN
}
