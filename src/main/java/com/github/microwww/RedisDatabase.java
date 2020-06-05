package com.github.microwww;

import com.github.microwww.database.AbstractValueData;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RedisDatabase {

    ConcurrentMap<byte[], AbstractValueData<?>> map = new ConcurrentHashMap();
    ReadWriteLock lock = new ReentrantReadWriteLock();


}
