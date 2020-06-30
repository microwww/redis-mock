package com.github.microwww.redis.database;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class SetData extends AbstractValueData<Set<Bytes>> {
    private final Set<Bytes> origin;

    public SetData() {
        this(NEVER_EXPIRE);
    }

    public SetData(int exp) {
        this(new ConcurrentSkipListSet<>(), exp);
    }

    public SetData(Set<Bytes> origin, int exp) {
        this.origin = origin;
        this.data = Collections.unmodifiableSet(this.origin);
        this.expire = exp;
    }

    //SADD
    public synchronized int add(byte[]... bytes) {
        int i = 0;
        for (byte[] a : bytes) {
            boolean success = origin.add(new Bytes(a));
            if (success) {
                i++;
            }
        }
        return i;
    }

    //SCARD
    //SDIFF
    public Set<Bytes> diff(RedisDatabase db, HashKey[] ms, int off) {
        Set<Bytes> collect = new HashSet<>();
        collect.addAll(this.getData());
        for (int i = off; i < ms.length; i++) {
            Optional<SetData> sd = db.get(ms[i], SetData.class);
            sd.ifPresent(ee -> { //
                collect.removeAll(ee.getData());
            });
        }
        return collect;
    }

    //SDIFFSTORE
    public Set<Bytes> diffStore(RedisDatabase db, HashKey[] ms) {
        Set<Bytes> diff = this.diff(db, ms, 2);
        SetData setData = new SetData();
        setData.origin.addAll(diff);
        db.put(ms[0], setData);
        return diff;
    }

    //SINTER
    //SINTERSTORE
    //SISMEMBER
    //SMEMBERS
    //SMOVE
    public synchronized boolean remove(byte[] o) {
        return origin.remove(o);
    }

    //SPOP
    public synchronized Optional<Bytes> pop() {
        Optional<Bytes> bytes = this.randMember();
        bytes.ifPresent(this.origin::remove);
        return bytes;
    }

    //SRANDMEMBER
    public synchronized Optional<Bytes> randMember() {
        Iterator<Bytes> it = origin.iterator();
        if (it.hasNext()) {
            Bytes e = it.next();
            return Optional.of(e);
        }
        return Optional.empty();
    }
    //SREM
    //SUNION
    //SUNIONSTORE
    //SSCAN
}
