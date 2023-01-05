package com.github.microwww.redis.protocal;

import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.protocal.jedis.Protocol;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RedisOutputProtocol {
    protected final JedisOutputStream out;

    public RedisOutputProtocol(JedisOutputStream out) {
        this.out = out;
    }

    public void writer(String simple) throws IOException {
        out.write(Protocol.PLUS_BYTE);
        out.writeAsciiCrLf(simple);
    }

    public void writerError(Level level, String simple) throws IOException {
        out.write(Protocol.MINUS_BYTE);
        out.writeAsciiCrLf(level.name() + " " + simple);
    }

    public void writer(int val) throws IOException {
        out.write(Protocol.COLON_BYTE);
        out.writeIntCrLf(val);
    }

    public void writer(long val) throws IOException {
        out.write(Protocol.COLON_BYTE);
        out.writeAsciiCrLf(val + "");
    }

    public void writerNull() throws IOException {
        out.write(Protocol.DOLLAR_BYTE);
        out.writeIntCrLf(-1);
    }

    public void writer(Bytes val) throws IOException {
        writer(val == null ? null : val.getBytes());
    }

    public void writer(byte[] val) throws IOException {
        out.write(Protocol.DOLLAR_BYTE);
        if (val == null) {
            out.writeIntCrLf(-1);
        } else {
            out.writeIntCrLf(val.length);
            out.write(val);
            out.writeCrLf();
        }
    }

    public void writerNested(byte[] start, byte[][] args) throws IOException {
        out.write(Protocol.ASTERISK_BYTE);
        out.writeIntCrLf(2);
        out.write(Protocol.DOLLAR_BYTE);
        out.writeIntCrLf(start.length);
        out.write(start);
        out.writeCrLf();
        writerMulti(args);
    }

    public void writerMulti(byte[]... args) throws IOException {
        writerComplexData(Protocol.ASTERISK_BYTE, args);
    }

    public void sendToSubscribe(Object... args) throws IOException {
        writerComplexData(Protocol.ASTERISK_BYTE, args);
    }

    public void writerComplex(Object... args) throws IOException {
        writerComplexData(Protocol.ASTERISK_BYTE, args);
    }

    protected void writerComplexData(byte prefix, Object[] args) throws IOException {
        if (args == null) {
            writerNull();
            return;
        }
        out.write(prefix);
        out.writeIntCrLf(args.length);

        for (Object arg : args) {
            if (arg == null) {
                writerNull();
                continue;
            }
            if (arg instanceof byte[]) {
                byte[] val = (byte[]) arg;
                writer(val);
            } else if (arg instanceof Bytes) {
                writer(((Bytes) arg).getBytes());
            } else if (arg instanceof Number) {
                writer(((Number) arg).longValue());
            } else {
                throw new UnsupportedEncodingException("Not support type: " + arg.getClass().getName());
            }
        }
    }

    public void flush() throws IOException {
        out.flush();
    }

    public JedisOutputStream getOut() {
        return out;
    }

    public enum Level {
        ERR, NOPROTO, WARN, FAIL
    }
}
