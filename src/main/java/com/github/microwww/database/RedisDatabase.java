package com.github.microwww.database;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RedisDatabase {

    ConcurrentMap<byte[], AbstractValueData<?>> map = new ConcurrentHashMap();
    ReadWriteLock lock = new ReentrantReadWriteLock();

    public void put(byte[] key, byte[] val) {
        map.put(key, new DataByte(val, AbstractValueData.NEVER_EXPIRE));
    }

}
