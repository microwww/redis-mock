package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.*;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.util.NotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Optional;

public class SortedSetOperation extends AbstractOperation {

    //ZADD
    public void zadd(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        ExpectRedisRequest[] args = request.getArgs();
        SortedSetData ss = getOrCreate(request);
        Member[] ms = new Member[args.length / 2];
        for (int i = 1; i < args.length; i += 2) {
            BigDecimal dec = args[i].byteArray2decimal();
            byte[] ba = args[i].getByteArray();
            ms[i - 1] = new Member(ba, dec);
        }
        int count = ss.add(ms);
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //ZCARD
    public void zcard(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        HashKey hk = request.getArgs()[0].byteArray2hashKey();
        Optional<SortedSetData> ss = request.getDatabase().get(hk, SortedSetData.class);
        int size = ss.map(e -> e.getData().size()).orElse(0);
        RedisOutputProtocol.writer(request.getOutputStream(), size);
    }

    //ZCOUNT
    public void zcount(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey hk = args[0].byteArray2hashKey();
        Optional<SortedSetData> ss = request.getDatabase().get(hk, SortedSetData.class);
        if(ss.isPresent()) {
            BigDecimal min = args[1].byteArray2decimal();
            BigDecimal max = args[2].byteArray2decimal();
            ss.get().getData();
        }
        RedisOutputProtocol.writer(request.getOutputStream(), 0);
    }
    //ZINCRBY
    //ZRANGE
    //ZRANGEBYSCORE
    //ZRANK
    //ZREM
    //ZREMRANGEBYRANK
    //ZREMRANGEBYSCORE
    //ZREVRANGE
    //ZREVRANGEBYSCORE
    //ZREVRANK
    //ZSCORE
    //ZUNIONSTORE
    //ZINTERSTORE
    //ZSCAN

    @NotNull
    private SortedSetData getOrCreate(RedisRequest request) {
        HashKey key = new HashKey(request.getArgs()[0].getByteArray());
        return request.getDatabase().getOrCreate(key, SortedSetData::new);
    }
}
