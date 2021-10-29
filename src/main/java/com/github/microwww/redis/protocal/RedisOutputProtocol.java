package com.github.microwww.redis.protocal;

import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.protocal.jedis.Protocol;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RedisOutputProtocol {

    public static void writer(JedisOutputStream out, String simple) throws IOException {
        out.write(Protocol.PLUS_BYTE);
        out.writeAsciiCrLf(simple);
    }

    public static void writerError(JedisOutputStream out, Level level, String simple) throws IOException {
        out.write(Protocol.MINUS_BYTE);
        out.writeAsciiCrLf(level.name() + " " + simple);
    }

    public static void writer(JedisOutputStream out, int val) throws IOException {
        out.write(Protocol.COLON_BYTE);
        out.writeIntCrLf(val);
    }

    public static void writer(JedisOutputStream out, long val) throws IOException {
        out.write(Protocol.COLON_BYTE);
        out.writeAsciiCrLf(val + "");
    }

    public static void writerNull(JedisOutputStream out) throws IOException {
        out.write(Protocol.DOLLAR_BYTE);
        out.writeIntCrLf(-1);
    }

    public static void writer(JedisOutputStream out, Bytes val) throws IOException {
        writer(out, val == null ? null : val.getBytes());
    }

    public static void writer(JedisOutputStream out, byte[] val) throws IOException {
        out.write(Protocol.DOLLAR_BYTE);
        if (val == null) {
            out.writeIntCrLf(-1);
        } else {
            out.writeIntCrLf(val.length);
            out.write(val);
            out.writeCrLf();
        }
    }

    public static void writerNested(JedisOutputStream out, byte[] start, byte[][] args) throws IOException {
        out.write(Protocol.ASTERISK_BYTE);
        out.writeIntCrLf(2);
        out.write(Protocol.DOLLAR_BYTE);
        out.writeIntCrLf(start.length);
        out.write(start);
        out.writeCrLf();
        writerMulti(out, args);
    }

    public static void writerMulti(JedisOutputStream out, byte[]... args) throws IOException {
        writerComplex(out, args);
    }

    public static void writerComplex(JedisOutputStream out, Object... args) throws IOException {
        if (args == null) {
            writerNull(out);
            return;
        }
        out.write(Protocol.ASTERISK_BYTE);
        out.writeIntCrLf(args.length);

        for (Object arg : args) {
            if (arg == null) {
                writerNull(out);
                continue;
            }
            if (arg instanceof byte[]) {
                byte[] val = (byte[]) arg;
                writer(out, val);
            } else if (arg instanceof Bytes) {
                writer(out, ((Bytes) arg).getBytes());
            } else if (arg instanceof Number) {
                writer(out, ((Number) arg).longValue());
            } else {
                throw new UnsupportedEncodingException("Not support type: " + arg.getClass().getName());
            }
        }
    }

    public enum Level {
        ERR, WARN, FAIL
    }
}
