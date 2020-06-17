package com.github.microwww.protocal;

import redis.clients.jedis.Protocol;
import redis.clients.util.RedisOutputStream;

import java.io.IOException;

public class RedisOutputProtocol {

    public static void writer(RedisOutputStream out, String simple) throws IOException {
        out.write(Protocol.PLUS_BYTE);
        out.writeAsciiCrLf(simple);
        out.flush();
    }

    public static void writerError(RedisOutputStream out, String level, String simple) throws IOException {
        out.write(Protocol.MINUS_BYTE);
        out.writeAsciiCrLf(level + " " + simple);
        out.flush();
    }
}
