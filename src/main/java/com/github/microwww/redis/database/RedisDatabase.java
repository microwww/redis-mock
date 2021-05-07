package com.github.microwww.redis.database;

import com.github.microwww.redis.util.Assert;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class RedisDatabase implements DataLock {

    ConcurrentMap<HashKey, AbstractValueData<?>> map = new ConcurrentHashMap<>();

    public void put(HashKey key, byte[] val) {
        map.put(key, new ByteData(val, AbstractValueData.NEVER_EXPIRE));
    }

    public AbstractValueData<?> put(HashKey key, AbstractValueData<?> data) {
        return map.put(key, data);
    }

    public synchronized <U, T extends AbstractValueData<U>> T putIfAbsent(HashKey key, T data) {
        AbstractValueData<?> dt = map.get(key);
        if (dt != null && dt.isExpired()) {
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

    public synchronized <U, T extends AbstractValueData<U>> T getOrCreate(HashKey key, Supplier<T> fun) {
        Optional<T> opt = (Optional<T>) this.get(key);
        if (!opt.isPresent()) {
            T v = fun.get();
            T t = this.putIfAbsent(key, v);
            opt = Optional.of(t == null ? v : t);
        }
        return opt.get();
    }

    public synchronized Optional<AbstractValueData<?>> setExpire(HashKey key, long expire) {
        Assert.isTrue(expire >= 0, "Time >= 0");
        Optional<AbstractValueData<?>> val = this.get(key);
        val.ifPresent(e -> {
            e.setExpire(expire);
            if (e.isExpired()) {
                this.remove(key);
            }
        });
        return val;
    }

    public Optional<AbstractValueData<?>> get(HashKey key) {
        AbstractValueData<?> val = map.get(key);
        if (val != null && val.isExpired()) {
            map.remove(key, val);
            return Optional.empty();
        }
        return Optional.ofNullable(val);
    }

    public Map<HashKey, AbstractValueData<?>> getUnmodifiableMap() {
        return Collections.unmodifiableMap(map);
    }

    public int getMapSize() {
        return map.size();
    }

    //DEL
    public synchronized AbstractValueData<?> remove(HashKey key) {
        return map.remove(key);
    }

    public void clear() {
        this.map.clear();
    }

    //DUMP
    //EXISTS
    //EXPIRE
    //EXPIREAT
    //KEYS
    //MIGRATE
    //MOVE
    //OBJECT
    //PERSIST
    //PEXPIRE
    //PEXPIREAT
    //PTTL
    //RANDOMKEY
    //RENAME
    //RENAMENX
    //RESTORE
    //SORT
    //TTL
    //TYPE
    //SCAN
}
