package com.github.microwww.protocal.operation;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.database.AbstractValueData;
import com.github.microwww.database.HashKey;
import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;

import java.io.IOException;

public class KeyOperation extends AbstractOperation {

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
