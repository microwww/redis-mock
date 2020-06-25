package com.github.microwww.redis.database;

import com.github.microwww.redis.ExpectRedisRequest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashData extends AbstractValueData<Map<HashKey, byte[]>> implements DataLock {

    private final Map<HashKey, byte[]> origin;

    public HashData() {
        this(NEVER_EXPIRE);
    }

    public HashData(long exp) {
        this(new ConcurrentHashMap<>(), exp);
    }

    public HashData(Map<HashKey, byte[]> origin, long exp) {
        this.origin = origin;
        this.data = Collections.unmodifiableMap(origin);
        this.expire = exp;
    }

    //HDEL
    public synchronized byte[] remove(HashKey key) {
        return origin.remove(key);
    }

    //HEXISTS
    //HGET
    //HGETALL

    //HINCRBY
    public synchronized byte[] incrBy(HashKey key, int inc) {
        byte[] bt = origin.get(key);
        BigInteger bi;
        if (bt == null) {
            bi = BigInteger.ZERO;
        } else {
            bi = new BigInteger(new String(bt));
        }
        bi = bi.add(BigInteger.valueOf(inc));
        byte[] val = bi.toString().getBytes();
        origin.put(key, val);
        return val;
    }

    //HINCRBYFLOAT
    public synchronized byte[] incrByFloat(HashKey key, BigDecimal inc) {
        byte[] bt = origin.get(key);
        BigDecimal bi;
        if (bt == null) {
            bi = BigDecimal.ZERO;
        } else {
            bi = new BigDecimal(new String(bt));
        }
        bi = bi.add(inc);
        byte[] bytes = bi.toPlainString().getBytes();
        origin.put(key, bytes);
        return bytes;
    }

    //HKEYS
    //HLEN
    //HMGET
    //HMSET
    public synchronized int multiSet(ExpectRedisRequest[] kvs, int offset) {
        for (int i = offset; i < kvs.length; i += 2) {
            this.origin.put(kvs[i].byteArray2hashKey(), kvs[i + 1].getByteArray());
        }
        return kvs.length - offset / 2;
    }

    //HSET
    public synchronized byte[] put(HashKey key, byte[] bytes) {
        return origin.put(key, bytes);
    }

    //HSETNX
    public synchronized byte[] putIfAbsent(HashKey key, byte[] bytes) {
        return origin.putIfAbsent(key, bytes);
    }
    //HVALS
    //HSCAN
}
