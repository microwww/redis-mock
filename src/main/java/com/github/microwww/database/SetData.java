package com.github.microwww.database;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class SetData extends AbstractValueData<Set<byte[]>> {
    private final Set<byte[]> origin;

    public SetData() {
        this.origin = new ConcurrentSkipListSet<>(ByteData.COMPARATOR);
        this.data = Collections.unmodifiableSet(this.origin);
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
