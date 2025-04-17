package com.github.microwww.redis.protocal;

import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.protocal.message.Type;

import java.io.IOException;

public class RespV3 extends RedisOutputProtocol {
    public RespV3(JedisOutputStream out) {
        super(out);
    }

    @Override
    public void writerNull() throws IOException {
        out.write((byte) Type.NULL.prefix);
        out.writeCrLf();
    }

    public void sendToSubscribe(Object... args) throws IOException {
        writerComplexData((byte) Type.PUSH.prefix, args);
    }
}
