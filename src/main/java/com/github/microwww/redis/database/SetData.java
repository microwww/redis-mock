package com.github.microwww.redis.database;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SetData extends AbstractValueData<Set<Bytes>> implements DataLock {
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
    public synchronized Set<Bytes> diff(RedisDatabase db, HashKey[] ms, int off) {
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
    public synchronized Set<Bytes> diffStore(RedisDatabase db, HashKey[] ms) {
        Set<Bytes> diff = this.diff(db, ms, 2);
        SetData setData = new SetData();
        setData.origin.addAll(diff);
        db.put(ms[0], setData);
        return diff;
    }

    //SINTER
    public synchronized Set<Bytes> inter(RedisDatabase db, HashKey[] ms, int off) {
        Map<Bytes, List<Bytes>> collect = this.getData().stream().collect(Collectors.groupingBy(Function.identity()));
        for (int i = off; i < ms.length; i++) {
            Optional<SetData> sd = db.get(ms[i], SetData.class);
            if (!sd.isPresent()) {
                return Collections.emptySet();
            }
            sd.get().getData().forEach(e -> {
                List<Bytes> list = collect.get(e);
                if (list != null) {
                    list.add(e);
                }
            });
        }
        return collect.values().stream().filter(e -> e.size() == ms.length - off).map(e -> e.get(0)).collect(Collectors.toSet());
    }

    //SINTERSTORE
    public synchronized Set<Bytes> interStore(RedisDatabase db, HashKey[] ms) {
        Set<Bytes> inter = this.inter(db, ms, 2);
        SetData data = new SetData();
        data.origin.addAll(inter);
        db.put(ms[0], data);
        return data.getData();
    }

    //SISMEMBER
    //SMEMBERS
    //SMOVE
    public boolean move(RedisDatabase db, HashKey dest, Bytes member) {
        boolean remove = this.origin.remove(member);
        if (remove) {
            SetData oc = db.getOrCreate(dest, SetData::new);
            oc.sync(() -> {// add
                return oc.origin.add(member);
            });
        }
        return remove;
    }

    public synchronized int remove(Bytes... os) {
        int count = 0;
        for (Bytes o : os) {
            boolean rm = origin.remove(o);
            if (rm) {
                count++;
            }
        }
        return count;
    }

    //SPOP
    public synchronized Optional<Bytes> pop() {
        Optional<Bytes> bytes = this.randMember();
        bytes.ifPresent(this.origin::remove);
        return bytes;
    }

    //SRANDMEMBER
    public synchronized Optional<Bytes> randMember() {
        int exchange = (int) (Math.random() * this.origin.size());
        Iterator<Bytes> it = origin.iterator();
        for (int i = 0; it.hasNext(); i++) {
            Bytes next = it.next();
            if (i == exchange) {
                return Optional.of(next);
            }
        }
        return Optional.empty();
    }

    public List<Bytes> exchange(int len) {
        List<Bytes> list = new ArrayList<>(this.origin);
        for (int i = 0; i < len; i++) {
            int exchange = (int) (Math.random() * list.size());
            list.set(i, list.set(exchange, list.get(i)));
        }
        return list.subList(0, len);
    }

    public List<Bytes> random(int len) {
        List<Bytes> list = new ArrayList<>(this.origin);
        List<Bytes> res = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            int ex = (int) (Math.random() * list.size());
            res.add(list.get(ex));
        }
        return res;
    }
    //SREM
    //SUNION
    //SUNIONSTORE
    //SSCAN
}
