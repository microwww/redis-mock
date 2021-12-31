package com.github.microwww.redis.protocal;

import com.github.microwww.redis.protocal.jedis.JedisOutputStream;

public class RespV2 extends RedisOutputProtocol {
    public RespV2(JedisOutputStream out) {
        super(out);
    }
}
