package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.HashData;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;

import java.io.IOException;
import java.util.Optional;

public class HashOperation extends AbstractOperation {

    public void hdel(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(1);
        Optional<HashData> data = this.getMap(request);
        int count = 0;
        if (data.isPresent()) {
            HashData e = data.get();
            ExpectRedisRequest[] args = request.getArgs();
            for (int i = 1; i < args.length; i++) {
                HashKey hk = new HashKey(args[i].getByteArray());
                byte[] remove = e.remove(hk);
                if (remove != null) {
                    count++;
                }
            }
        }
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    public void hget(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        byte[] hk = args[1].getByteArray();
        Optional<HashData> opt = this.getMap(request);
        if (opt.isPresent()) {
            byte[] dh = opt.get().getData().get(new HashKey(hk));
            if (dh != null) {
                RedisOutputProtocol.writer(request.getOutputStream(), dh);
                return;
            }
        }
        RedisOutputProtocol.writerNull(request.getOutputStream());
    }

    public void hset(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        byte[] key = args[0].getByteArray();

        Optional<HashData> opt = request.getDatabase().get(new HashKey(key), HashData.class);
        if (!opt.isPresent()) {
            HashData def = new HashData();
            HashData data = request.getDatabase().putIfAbsent(new HashKey(key), def);
            if (data == null) {
                data = def;
            }
            opt = Optional.of(data);
        }

        HashData data = opt.get();
        byte[] hk = args[1].getByteArray();
        byte[] val = args[2].getByteArray();
        byte[] origin = data.getData().get(new HashKey(hk));
        data.put(new HashKey(hk), val);

        // new: 1, over-write: 0
        RedisOutputProtocol.writer(request.getOutputStream(), origin == null ? 1 : 0);
    }

    private Optional<HashData> getMap(RedisRequest request) {
        ExpectRedisRequest[] args = request.getArgs();
        byte[] key = args[0].getByteArray();
        return request.getDatabase().get(new HashKey(key), HashData.class);
    }
}
