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
    public synchronized boolean add(byte[] bytes) {
        return origin.add(bytes);
    }

    //SCARD
    //SDIFF
    //SDIFFSTORE
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
