package com.github.microwww.database;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RedisDatabase {

    ConcurrentMap<HashKey, AbstractValueData<?>> map = new ConcurrentHashMap<>();
    ReadWriteLock lock = new ReentrantReadWriteLock();

    public void put(HashKey key, byte[] val) {
        map.put(key, new ByteData(val, AbstractValueData.NEVER_EXPIRE));
    }

    public <U, T extends AbstractValueData<U>> T putIfAbsent(HashKey key, T data) {
        AbstractValueData<?> dt = map.get(key);
        if (dt !=null && dt.isExpired()) {
            map.remove(key, dt);
        }
        return (T) map.putIfAbsent(key, data);
    }

    public Optional<ByteData> getBytes(HashKey key) {
        return this.get(key, ByteData.class);
    }

    public <U, T extends AbstractValueData<U>> Optional<T> get(HashKey key, Class<T> clazz) {
        return this.get(key).map(e -> (T) e);
    }

    public Optional<AbstractValueData<?>> get(HashKey key) {
        AbstractValueData<?> val = map.get(key);
        if (val != null && val.isExpired()) {
            map.remove(key, val);
            return Optional.empty();
        }
        return Optional.ofNullable(val);
    }

    public AbstractValueData<?> remove(HashKey key) {
        return map.remove(key);
    }

}
