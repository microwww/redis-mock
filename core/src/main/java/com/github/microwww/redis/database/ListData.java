package com.github.microwww.redis.database;

import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class ListData extends AbstractValueData<List<Bytes>> implements DataLock {
    private static final Logger log = LogFactory.getLogger(ListData.class);

    private final ChangeObservable addEvent = new ChangeObservable();
    private final List<Bytes> origin;

    public ListData() {
        this(NEVER_EXPIRE);
    }

    public ListData(int exp) {
        this(new ArrayList<>(), exp);
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

    public void subscribe(Observer sub) {
        addEvent.subscribe(sub);
    }

    public void unsubscribe(Observer sub) {
        addEvent.deleteObserver(sub);
    }

    //BLPOP
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
            this.version.incrementAndGet();
            addEvent.publish(this);
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
        Bytes remove = origin.remove(index);
        this.version.incrementAndGet();
        return remove;
    }

    //LLEN
    //LPOP
    public synchronized Optional<Bytes> leftPop() {
        Bytes rm = null;
        if (!origin.isEmpty()) {
            rm = this.remove(0);
        }
        return Optional.ofNullable(rm);
    }

    //LPUSH
    public synchronized void leftAdd(byte[]... bytes) {
        for (byte[] a : bytes) {
            origin.add(0, new Bytes(a));
        }
        if (bytes.length > 0) {
            this.version.incrementAndGet();
        }
        addEvent.publish(this);
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
        if (ct > 0) {
            this.version.incrementAndGet();
        }
        return ct;
    }

    //LSET
    public synchronized Bytes set(int index, byte[] element) {
        Bytes set = origin.set(index, new Bytes(element));
        this.version.incrementAndGet();
        return set;
    }

    //LTRIM
    public synchronized void trim(int start, int includeStop) {
        int from = index(start);
        int to = index(includeStop);
        int max = this.origin.size();
        if (from >= max || from > to) {
            this.origin.clear();
            this.version.incrementAndGet();
            return;
        }
        for (int i = this.origin.size() - 1; i >= 0; i--) {
            if (i < from) {
                this.remove(i);
            }
            if (i > to) {
                this.remove(i);
            }
        }
    }

    //RPOP
    public synchronized Optional<Bytes> rightPop() {
        Bytes or = null;
        if (!this.origin.isEmpty()) {
            or = this.remove(origin.size() - 1);
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
        this.version.incrementAndGet();
        this.addEvent.publish(this);
    }
    //RPUSHX

    public static class ChangeObservable extends Observable {
        public void publish(ListData list) {
            this.setChanged();
            this.notifyObservers(list);
        }

        public void subscribe(Observer o) {
            this.addObserver(o);
        }
    }
}
