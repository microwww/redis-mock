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

    @Override
    public String getType() {
        return "set";
    }

    //SADD
    public synchronized int add(Bytes... bytes) {
        int i = 0;
        for (Bytes a : bytes) {
            boolean success = origin.add(a);
            if (success) {
                i++;
            }
        }
        if (bytes.length > 0) {
            this.version.incrementAndGet();
        }
        return i;
    }

    //SCARD
    //SDIFF
    public synchronized Set<Bytes> diff(RedisDatabase db, HashKey[] ms) {
        Set<Bytes> origin = new HashSet<>(this.origin);
        for (HashKey m : ms) {
            Optional<SetData> sd = db.get(m, SetData.class);
            sd.ifPresent(ee -> { //
                origin.removeAll(ee.getData());
            });
        }
        return origin;
    }

    //SDIFFSTORE
    public synchronized SetData diffStore(RedisDatabase db, HashKey dest, HashKey[] ms) {
        Set<Bytes> diff = this.diff(db, ms);
        SetData target = new SetData();
        db.put(dest, target);
        target.origin.addAll(diff);
        return target;
    }

    //SINTER
    //SINTERSTORESINTERSTORE
    public synchronized Set<Bytes> interStore(RedisDatabase db, HashKey[] ms) {
        Optional<SetData> first = db.get(ms[0], SetData.class);
        if (!first.isPresent()) {
            return Collections.emptySet();
        }
        Map<Bytes, List<Bytes>> collect = first.get().getData().stream().collect(Collectors.groupingBy(Function.identity()));
        for (int i = 1; i < ms.length; i++) {
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
        collect.forEach((k, v) -> {
            if (v.size() == ms.length) {
                this.origin.add(k);
            }
        });
        return this.getData();
    }

    //SISMEMBER
    //SMEMBERS
    //SMOVE
    public boolean move(RedisDatabase db, HashKey dest, Bytes member) {
        boolean remove = this.origin.remove(member);
        if (remove) {
            this.version.incrementAndGet();
            SetData oc = db.getOrCreate(dest, SetData::new);
            oc.sync(() -> {// add
                return oc.origin.add(member);
            });
        }
        return remove;
    }

    //SPOP
    public synchronized List<Bytes> pop(int count) {
        List<Bytes> bytes = this.randMember(count);
        this.origin.removeAll(bytes);
        if (!bytes.isEmpty()) {
            this.version.incrementAndGet();
        }
        return bytes;
    }

    /**
     * @param len len
     * @return no repeat
     */
    //SRANDMEMBER
    public List<Bytes> randMember(int len) {
        List<Bytes> list = new ArrayList<>(this.origin);
        for (int i = 0; i < len && i < list.size(); i++) {
            int exchange = (int) (Math.random() * list.size());
            list.set(i, list.set(exchange, list.get(i)));
        }
        return list.subList(0, len);
    }

    /**
     * @param len length
     * @return There is duplicate data
     */
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
    public synchronized int removeAll(Bytes... os) {
        int count = 0;
        for (Bytes o : os) {
            boolean rm = origin.remove(o);
            if (rm) {
                count++;
            }
        }
        if (count > 0) {
            this.version.incrementAndGet();
        }
        return count;
    }

    //SUNION
    //SUNIONSTORE
    public synchronized Set<Bytes> union(RedisDatabase db, HashKey[] keys) {
        for (int i = 0; i < keys.length; i++) {
            db.get(keys[i], SetData.class).ifPresent(e -> {
                this.origin.addAll(e.getData());
            });
        }
        return this.getData();
    }
    //SSCAN
}
