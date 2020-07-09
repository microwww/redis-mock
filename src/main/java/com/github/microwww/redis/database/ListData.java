package com.github.microwww.redis.database;

import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ListData extends AbstractValueData<List<Bytes>> implements DataLock {
    private static final Logger log = LogFactory.getLogger(ListData.class);

    private final List<CountDownLatch> latches = new LinkedList<>(); // !! 该对象需要现成安全 !!!
    private final List<Bytes> origin;

    public ListData() {
        this(NEVER_EXPIRE);
    }

    public ListData(int exp) {
        this(new CopyOnWriteArrayList<>(), exp);
    }

    public ListData(List<Bytes> origin, int exp) {
        this.origin = origin;
        this.data = Collections.unmodifiableList(origin);
        this.expire = exp;
    }

    @Override
    public String getType() {
        return "list";
    }

    //BLPOP
    public synchronized Optional<Bytes> blockPop(CountDownLatch latch, Function<ListData, Optional<Bytes>> fun) {
        if (origin.isEmpty()) {
            if (!this.latches.contains(latch)) {
                this.latches.add(latch);
                log.debug("Lock {}", this);
            }
            return Optional.empty();
        }
        return fun.apply(this);
    }

    public synchronized void removeCountDownLatch(CountDownLatch latch) {
        this.latches.remove(latch);
    }
    //BRPOP
    //BRPOPLPUSH

    //LINDEX
    public synchronized Optional<Bytes> getByIndex(int index) {
        int max = origin.size();
        index = index(index);
        if (index >= max) {
            return Optional.empty();
        }
        return Optional.ofNullable(this.origin.get(index));
    }

    //LINSERT
    public synchronized boolean findAndOffsetInsert(byte[] pivot, int offset, byte[] value) {
        int index = this.indexOf(pivot);
        if (index >= 0) {
            this.origin.add(index + offset, new Bytes(value));
            this.latch();
            return true;
        }
        return false;
    }

    public synchronized int indexOf(byte[] val) {
        int index = -1;
        for (int i = 0; i < this.origin.size(); i++) {
            Bytes bytes = this.origin.get(i);
            if (Bytes.eq(bytes, val)) {
                index = i;
                break;
            }
        }
        return index;
    }

    public synchronized Bytes remove(int index) {
        return origin.remove(index);
    }

    //LLEN
    //LPOP
    public synchronized Optional<Bytes> leftPop() {
        Bytes rm = null;
        if (!origin.isEmpty()) {
            rm = origin.remove(0);
        }
        return Optional.ofNullable(rm);
    }

    //LPUSH
    public synchronized void leftAdd(byte[]... bytes) {
        for (byte[] a : bytes) {
            origin.add(0, new Bytes(a));
        }
        this.latch();
    }

    //LPUSHX
    //LRANGE
    public synchronized byte[][] range(int from, int includeTo) {
        int max = origin.size();
        from = index(from);
        int to = index(includeTo) + 1;
        if (from >= max) {
            return new byte[0][];
        }
        int end = Math.min(max, to);
        byte[][] byt = new byte[end - from][];
        for (int i = 0; i < byt.length; i++) {
            byt[i] = this.origin.get(from + i).getBytes();
        }
        return byt;
    }

    private synchronized int index(int index) {
        if (index < 0) {
            return this.origin.size() + index;
        }
        return index;
    }

    //LREM key count value
    public synchronized int remove(int count, byte[] val) {
        int ct = 0;
        if (count > 0) {
            int size = this.origin.size();
            for (int i = 0; i < size; i++) {
                if (Bytes.eq(this.origin.get(i), val)) {
                    this.origin.remove(i);
                    count--;
                    if (count <= 0) {
                        break;
                    }
                    i--;
                    size--;
                    ct++;
                }
            }
        } else {
            count = Math.abs(count);
            int size = this.origin.size();
            if (count == 0) {
                count = size;
            }
            for (int i = size - 1; i >= 0; i--) {
                if (Bytes.eq(this.origin.get(i), val)) {
                    this.origin.remove(i);
                    count--;
                    ct++;
                    if (count <= 0) {
                        break;
                    }
                }
            }
        }
        return ct;
    }

    //LSET
    public synchronized Bytes set(int index, byte[] element) {
        return origin.set(index, new Bytes(element));
    }

    //LTRIM
    public synchronized void trim(int start, int includeStop) {
        int from = index(start);
        int to = index(includeStop);
        int max = this.origin.size();
        if (from >= max || from > to) {
            this.origin.clear();
            return;
        }
        for (int i = this.origin.size() - 1; i >= 0; i--) {
            if (i < from) {
                this.origin.remove(i);
            }
            if (i > to) {
                this.origin.remove(i);
            }
        }
    }

    //RPOP
    public synchronized Optional<Bytes> rightPop() {
        Bytes or = null;
        if (!this.origin.isEmpty()) {
            or = origin.remove(origin.size() - 1);
        }
        return Optional.ofNullable(or);
    }

    //RPOPLPUSH
    public synchronized Optional<Bytes> pop2push(RedisDatabase db, HashKey target) {
        return this.rightPop().map(e -> {
            ListData dest = db.getOrCreate(target, ListData::new);
            return dest.sync(() -> {
                dest.leftAdd(e.getBytes());
                return e;
            });
        });
    }

    //RPUSH
    public synchronized void rightAdd(byte[]... bytes) {
        this.origin.addAll(Arrays.stream(bytes).map(Bytes::new).collect(Collectors.toList()));
        this.latch();
    }
    //RPUSHX

    private synchronized void latch() {
        log.debug("Try Release {}, {}", this, latches.size());
        if (!latches.isEmpty()) {
            try {
                latches.remove(0).countDown();
            } catch (Exception e) { // ignore
            }
        }
    }
}
