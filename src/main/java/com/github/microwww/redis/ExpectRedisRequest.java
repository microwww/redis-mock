package com.github.microwww.redis;

import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.util.SafeEncoder;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class ExpectRedisRequest {
    private final Object origin;

    public ExpectRedisRequest(Object origin) {
        this.origin = origin;
    }

    public byte[] getByteArray() {
        if (origin instanceof byte[]) {
            byte[] ts = (byte[]) this.origin;
            return Arrays.copyOf(ts, ts.length);
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

    public Bytes toBytes() {
        return new Bytes((byte[]) this.origin);
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
            throw new IllegalArgumentException("Not your expect type : it is NULL");
        }
        return this;
    }

    public Object getOrigin() {
        return origin;
    }

    public static ExpectRedisRequest[] parseRedisData(Object o) {
        ExpectRedisRequest[] res = {null};
        if (o == null) {
            return res;
        } else if (o instanceof List) {
            List<?> list = (List<?>) o;
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

    @Override
    public String toString() {
        if (origin instanceof byte[]) {
            return SafeEncoder.encode((byte[]) origin);
        }
        return "origin:" + origin;
    }
}