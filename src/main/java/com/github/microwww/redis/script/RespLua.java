package com.github.microwww.redis.script;

import com.github.microwww.redis.protocal.RespV3;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;

import java.io.IOException;

public class RespLua extends RespV3 {
    public RespLua(JedisOutputStream out) {
        super(out);
    }

    @Override
    protected void writerUnsupportedEncodingComplexData(Object arg) throws IOException {
        writerNull();
    }
}
