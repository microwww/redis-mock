package com.github.microwww.protocal.operation;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.database.AbstractValueData;
import com.github.microwww.database.HashKey;
import com.github.microwww.database.RedisDatabase;
import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;
import com.github.microwww.util.Assert;

import java.io.IOException;
import java.util.Optional;

public class KeyOperation extends AbstractOperation {

    public void expire(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 2, "Must has tow arguments");
        HashKey key = new HashKey(args[0].getByteArray());
        int exp = Integer.parseInt(new String(args[1].getByteArray()));
        RedisDatabase db = request.getDatabase();
        Optional<AbstractValueData<?>> val = db.get(key);
        val.ifPresent(e -> {//
            e.setSecondsExpire(exp);
        });
        RedisOutputProtocol.writer(request.getOutputStream(), 1);
    }

    public void del(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(0);
        int count = 0;
        for (ExpectRedisRequest arg : request.getArgs()) {
            AbstractValueData<?> val = request.getDatabase().remove(new HashKey(arg.getByteArray()));
            if (val != null) {
                count++;
            }
        }
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

}
