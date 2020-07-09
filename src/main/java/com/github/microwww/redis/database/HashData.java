package com.github.microwww.redis.database;

import com.github.microwww.redis.ExpectRedisRequest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashData extends AbstractValueData<Map<HashKey, Bytes>> implements DataLock {

    private final Map<HashKey, Bytes> origin;

    public HashData() {
        this(NEVER_EXPIRE);
    }

    public HashData(long exp) {
        this(new ConcurrentHashMap<>(), exp);
    }

    public HashData(Map<HashKey, Bytes> origin, long exp) {
        this.origin = origin;
        this.data = Collections.unmodifiableMap(origin);
        this.expire = exp;
    }

    @Override
    public String getType() {
        return "hash";
    }

    //HDEL
    public synchronized Bytes remove(HashKey key) {
        if (origin.containsKey(key)) {
            this.version.incrementAndGet();
            return origin.remove(key);
        }
        return null;
    }

    //HEXISTS
    //HGET
    //HGETALL

    //HINCRBY
    public synchronized Bytes incrBy(HashKey key, int inc) {
        Bytes bt = origin.get(key);
        BigInteger bi;
        if (bt == null) {
            bi = BigInteger.ZERO;
        } else {
            bi = new BigInteger(bt.toString());
        }
        bi = bi.add(BigInteger.valueOf(inc));
        Bytes bytes = new Bytes(bi.toString().getBytes());
        origin.put(key, bytes);
        this.version.incrementAndGet();
        return bytes;
    }

    //HINCRBYFLOAT
    public synchronized Bytes incrByFloat(HashKey key, BigDecimal inc) {
        Bytes bt = origin.get(key);
        BigDecimal bi;
        if (bt == null) {
            bi = BigDecimal.ZERO;
        } else {
            bi = new BigDecimal(bt.toString());
        }
        bi = bi.add(inc);
        Bytes bytes = new Bytes(bi.toPlainString().getBytes());
        origin.put(key, bytes);
        this.version.incrementAndGet();
        return bytes;
    }

    //HKEYS
    //HLEN
    //HMGET
    //HMSET
    public synchronized int multiSet(ExpectRedisRequest[] kvs, int offset) {
        for (int i = offset; i < kvs.length; i += 2) {
            this.origin.put(kvs[i].byteArray2hashKey(), kvs[i + 1].toBytes());
        }
        this.version.incrementAndGet();
        return kvs.length - offset / 2;
    }

    //HSET
    public synchronized Bytes put(HashKey key, byte[] bytes) {
        this.version.incrementAndGet();
        return origin.put(key, new Bytes(bytes));
    }

    //HSETNX
    public synchronized Bytes putIfAbsent(HashKey key, byte[] bytes) {
        Bytes val = origin.putIfAbsent(key, new Bytes(bytes));
        if (val == null) {
            this.version.incrementAndGet();
        }
        return val;
    }
    //HVALS
    //HSCAN
}
