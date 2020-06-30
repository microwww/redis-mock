package com.github.microwww.redis.database;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class SetData extends AbstractValueData<Set<byte[]>> {
    private final Set<byte[]> origin;

    public SetData() {
        this(NEVER_EXPIRE);
    }

    public SetData(int exp) {
        this(new ConcurrentSkipListSet<>(ByteData.COMPARATOR), exp);
    }

    public SetData(Set<byte[]> origin, int exp) {
        this.origin = origin;
        this.data = Collections.unmodifiableSet(this.origin);
        this.expire = exp;
    }

    //SADD
    public synchronized int add(byte[]... bytes) {
        int i = 0;
        for (byte[] a : bytes) {
            boolean success = origin.add(a);
            if (success) {
                i++;
            }
        }
        return i;
    }

    //SCARD
    //SDIFF
    public Set<byte[]> diff(RedisDatabase db, HashKey[] ms, int off) {
        Set<byte[]> collect = new TreeSet<>(ByteData.COMPARATOR);
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
    public Set<byte[]> diffStore(RedisDatabase db, HashKey[] ms) {
        Set<byte[]> diff = this.diff(db, ms, 2);
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
    public synchronized Optional<byte[]> pop() {
        Optional<byte[]> bytes = this.randMember();
        bytes.ifPresent(this.origin::remove);
        return bytes;
    }

    //SRANDMEMBER
    public synchronized Optional<byte[]> randMember() {
        Iterator<byte[]> it = origin.iterator();
        if (it.hasNext()) {
            byte[] e = it.next();
            return Optional.of(e);
        }
        return Optional.empty();
    }
    //SREM
    //SUNION
    //SUNIONSTORE
    //SSCAN
}
