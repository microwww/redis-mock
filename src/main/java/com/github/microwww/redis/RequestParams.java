package com.github.microwww.redis;

import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.protocal.NetPacket;
import com.github.microwww.redis.util.SafeEncoder;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class RequestParams {
    private final NetPacket origin;

    public RequestParams(byte[] origin) {
        this.origin = new NetPacket.Bulk(origin);
    }

    public RequestParams(NetPacket origin) {
        Object data = origin.getData();
        if (data == null) {
            throw new IllegalArgumentException("Request is NULL");
        } else if (data instanceof NetPacket[]) {
            throw new IllegalArgumentException("To ExpectRedisRequest[]");
        }
        this.origin = origin;
    }

    public static RequestParams[] convert(NetPacket packet) {
        Object data = packet.getData();
        if (data == null) {
            throw new IllegalArgumentException("Request is NULL");
        }
        if (data instanceof NetPacket[]) {
            NetPacket[] nps = (NetPacket[]) data;
            return Arrays.stream(nps).map(RequestParams::new).toArray(RequestParams[]::new);
        }
        return new RequestParams[]{new RequestParams(packet)};
    }

    public byte[] getByteArray() {
        Object origin = this.origin.getData();
        if (origin instanceof byte[]) {
            byte[] ts = (byte[]) origin;
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
        return new Bytes((byte[]) this.origin.getData());
    }

    public Long getLong() {
        if (origin instanceof NetPacket.BigInt) {
            return (Long) origin.getData();
        }
        throw new IllegalArgumentException("Not your expect type : " + origin);
    }

    public void isNull() {
        if (origin != null) {
            throw new IllegalArgumentException("Not your expect type : " + origin);
        }
    }

    public RequestParams isNotNull() {
        if (origin == null) {
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
            res[0] = new RequestParams((byte[]) o);
        } else if (o instanceof Long) {
            res[0] = new RequestParams(new NetPacket.BigInt((Long) o));
        } else {
            throw new IllegalArgumentException("Not support Expect type : " + o);
        }
        return res;
    }

    @Override
    public String toString() {
        Object origin = this.origin.getData();
        if (origin instanceof byte[]) {
            return SafeEncoder.encode((byte[]) origin);
        }
        return "origin:" + origin;
    }
}