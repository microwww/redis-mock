package com.github.microwww.redis;

import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.protocal.message.*;
import com.github.microwww.redis.util.SafeEncoder;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class RequestParams {
    private final RedisMessage origin;

    public RequestParams(RedisMessage origin) {
        this.origin = origin;
    }

    public static RequestParams[] convert(RedisMessage packet) {
        RedisMessage[] rm = packet.getRedisMessages();
        RequestParams[] rs = new RequestParams[rm.length];
        for (int i = 0; i < rm.length; i++) {
            rs[i] = new RequestParams(rm[i]);
        }
        return rs;
    }

    public byte[] getByteArray() {
        byte[] origin = this.origin.getBytes();
        if (origin == null) {
            return null;
        }
        return Arrays.copyOf(origin, origin.length);
    }

    public String getByteArray2string() {
        return SafeEncoder.encode(this.getByteArray());
    }

    public int byteArray2int() {
        return Integer.parseInt(SafeEncoder.encode(this.getByteArray()));
    }

    public long byteArray2long() {
        return Long.parseLong(SafeEncoder.encode(this.getByteArray()));
    }

    public BigDecimal byteArray2decimal() {
        return new BigDecimal(SafeEncoder.encode(this.getByteArray()));
    }

    public HashKey byteArray2hashKey() {
        return new HashKey(this.getByteArray());
    }

    public Bytes toBytes() {
        return new Bytes(this.origin.getBytes());
    }

    public Long getLong() {
        if (origin instanceof LongMessage) {
            return ((LongMessage) origin).toLong();
        }
        throw new IllegalArgumentException("Not your expect type : " + origin);
    }

    public RequestParams isNotNull() {
        if (origin == null || origin.getBytes() == null) {
            throw new IllegalArgumentException("Not your expect type : it is NULL");
        }
        return this;
    }

    public Object getOrigin() {
        return origin;
    }

    public static RequestParams[] parseRedisData(Object o) {
        RequestParams[] res = {null};
        if (o == null) {
            return res;
        } else if (o instanceof Collection) {
            List<?> list = (List<?>) o;
            res = new RequestParams[list.size()];
            for (int i = 0; i < list.size(); i++) {
                RequestParams[] ex = parseRedisData(list.get(i));
                if (ex.length != 1) {
                    throw new IllegalArgumentException("Only Support one line data !");
                }
                res[i] = ex[0];
            }
            return res;
        } else if (o instanceof byte[]) {
            res[0] = new RequestParams(new BytesMessage(Type.MULTI, (byte[]) o));
        } else if (o instanceof Long) {
            res[0] = new RequestParams(new BigIntMessage(Type.BigInt, SafeEncoder.encode(o.toString())));
        } else {
            throw new IllegalArgumentException("Not support Expect type : " + o);
        }
        return res;
    }

    @Override
    public String toString() {
        byte[] bts = this.origin.getBytes();
        if (bts != null) {
            return SafeEncoder.encode(bts);
        }
        return "origin:" + this.origin;
    }
}