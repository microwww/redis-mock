package com.github.microwww.redis.protocal;

import redis.clients.jedis.Protocol;
import redis.clients.util.RedisOutputStream;

import java.io.IOException;

public class RedisOutputProtocol {

    public static void writer(RedisOutputStream out, String simple) throws IOException {
        out.write(Protocol.PLUS_BYTE);
        out.writeAsciiCrLf(simple);
        out.flush();
    }

    public static void writerError(RedisOutputStream out, Level level, String simple) throws IOException {
        out.write(Protocol.MINUS_BYTE);
        out.writeAsciiCrLf(level.name() + " " + simple);
        out.flush();
    }

    public static void writer(RedisOutputStream out, int val) throws IOException {
        out.write(Protocol.COLON_BYTE);
        out.writeIntCrLf(val);
        out.flush();
    }

    public static void writer(RedisOutputStream out, long val) throws IOException {
        out.write(Protocol.COLON_BYTE);
        out.writeAsciiCrLf(val + "");
        out.flush();
    }

    public static void writerNull(RedisOutputStream out) throws IOException {
        out.write(Protocol.DOLLAR_BYTE);
        out.writeIntCrLf(-1);
        out.flush();
    }

    public static void writer(RedisOutputStream out, byte[] val) throws IOException {
        out.write(Protocol.DOLLAR_BYTE);
        out.writeIntCrLf(val.length);
        out.write(val);
        out.writeCrLf();
        out.flush();
    }

    public static void writerMulti(RedisOutputStream out, byte[]... args) throws IOException {
        out.write(Protocol.ASTERISK_BYTE);
        out.writeIntCrLf(args.length);

        for (byte[] val : args) {
            if (val == null) {
                out.write(Protocol.DOLLAR_BYTE);
                out.writeIntCrLf(-1);
                continue;
            }
            out.write(Protocol.DOLLAR_BYTE);
            out.writeIntCrLf(val.length);
            out.write(val);
            out.writeCrLf();
        }
        out.flush();
    }

    public enum Level {
        ERR, WARN, FAIL
    }
}
