package com.github.microwww.protocal.operation;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.database.DataHash;
import com.github.microwww.database.HashKey;
import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;

import java.io.IOException;
import java.util.Optional;

public class HashOperation extends AbstractOperation {

    public void hdel(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(1);
        Optional<DataHash> data = this.getMap(request);
        int count = 0;
        if (data.isPresent()) {
            DataHash e = data.get();
            ExpectRedisRequest[] args = request.getArgs();
            for (int i = 1; i < args.length; i++) {
                String hk = args[i].getByteArray2string();
                byte[] remove = e.getData().remove(new HashKey(hk));
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
        String hk = args[1].getByteArray2string();
        Optional<DataHash> opt = this.getMap(request);
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
        String key = args[0].getByteArray2string();

        Optional<DataHash> opt = request.getDatabase().get(new HashKey(key), DataHash.class);
        if (!opt.isPresent()) {
            DataHash def = new DataHash();
            DataHash data = request.getDatabase().putIfAbsent(new HashKey(key), def);
            if (data == null) {
                data = def;
            }
            opt = Optional.of(data == null ? def : data);
        }

        DataHash data = opt.get();
        String hk = args[1].getByteArray2string();
        byte[] val = args[2].getByteArray();
        byte[] origin = data.getData().get(new HashKey(hk));
        data.getData().put(new HashKey(hk), val);

        // new: 1, over-write: 0
        RedisOutputProtocol.writer(request.getOutputStream(), origin == null ? 1 : 0);
    }

    private Optional<DataHash> getMap(RedisRequest request) {
        ExpectRedisRequest[] args = request.getArgs();
        String key = args[0].getByteArray2string();
        return request.getDatabase().get(new HashKey(key), DataHash.class);
    }
}
