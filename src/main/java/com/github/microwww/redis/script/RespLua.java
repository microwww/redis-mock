package com.github.microwww.redis.script;

import com.github.microwww.redis.protocal.RespV2;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RespLua extends RespV2 {

    private ByteArrayOutputStream cache;

    public RespLua(ByteArrayOutputStream out) {
        super(new JedisOutputStream(out));
        this.cache = out;
    }

    public static RespLua create() {
        return new RespLua(new ByteArrayOutputStream(1 * 1024));
    }

    @Override
    protected void writerUnsupportedEncodingComplexData(Object arg) throws IOException {
        this.writerNull();
    }

    public byte[] getData() throws IOException {
        this.flush();
        return cache.toByteArray();
    }

    public void reset() throws IOException {
        this.flush();
        cache.reset();
    }
}
