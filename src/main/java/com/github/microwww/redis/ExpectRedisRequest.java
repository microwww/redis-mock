package com.github.microwww.redis;

import com.github.microwww.redis.database.HashKey;
import redis.clients.util.SafeEncoder;

import java.math.BigDecimal;
import java.util.List;

public class ExpectRedisRequest {
    private final Object origin;

    public ExpectRedisRequest(Object origin) {
        this.origin = origin;
    }

    public byte[] getByteArray() {
        if (origin instanceof byte[]) {
            return (byte[]) origin;
        }
        throw new IllegalArgumentException("Not your expect type : " + origin);
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

    public Long getLong() {
        if (origin instanceof Long) {
            return (Long) origin;
        }
        throw new IllegalArgumentException("Not your expect type : " + origin);
    }

    public void isNull() {
        if (origin != null) {
            throw new IllegalArgumentException("Not your expect type : " + origin);
        }
    }

    public ExpectRedisRequest isNotNull() {
        if (origin == null) {
            throw new IllegalArgumentException("Not your expect type : " + origin);
        }
        return this;
    }

    public <T> T expectNull(Class<T> c) {
        if (origin != null) {
            throw new IllegalArgumentException("Not your expect type : " + origin);
        }
        return null;
    }

    public Object getOrigin() {
        return origin;
    }

    public static ExpectRedisRequest[] parseRedisData(Object o) {
        ExpectRedisRequest[] res = {null};
        if (o == null) {
            return res;
        } else if (o instanceof List) {
            List list = (List) o;
            res = new ExpectRedisRequest[list.size()];
            for (int i = 0; i < list.size(); i++) {
                ExpectRedisRequest[] ex = parseRedisData(list.get(i));
                if (ex.length != 1) {
                    throw new IllegalArgumentException("Only Support one line data !");
                }
                res[i] = ex[0];
            }
            return res;
        } else if (o instanceof byte[]) {
            res[0] = new ExpectRedisRequest(o);
        } else if (o instanceof Long) {
            res[0] = new ExpectRedisRequest(o);
        } else {
            throw new IllegalArgumentException("Not support Expect type : " + o);
        }
        return res;
    }
}