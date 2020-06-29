package com.github.microwww.redis.database;

import com.github.microwww.redis.protocal.operation.SortedSetOperation;
import com.github.microwww.redis.util.Assert;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SortedSetData extends AbstractValueData<SortedSet<Member>> implements DataLock {
    private final ConcurrentSkipListSet<Member> origin;
    private final HashMap<HashKey, Member> unique = new HashMap<>();

    public SortedSetData() {
        this(NEVER_EXPIRE);
    }

    public SortedSetData(int exp) {
        this.origin = new ConcurrentSkipListSet<>(Member.COMPARATOR);
        this.data = Collections.unmodifiableSortedSet(this.origin);
        this.expire = exp;
    }

    public SortedSet<Member> getData(boolean desc) {
        if (desc) {
            return Collections.unmodifiableSortedSet(this.origin.descendingSet());
        } else {
            return this.data;
        }
    }

    //ZADD
    public synchronized int addOrReplace(Member... member) {
        int count = 0;
        for (Member m : member) {
            Member og = this.addElement(m);
            if (og == null) {
                count++;
            }
        }
        return count;
    }

    private synchronized Member addElement(Member member) {
        Assert.isNotNull(member.getMember(), "member.byte[] not null");
        origin.add(member);
        Member put = unique.put(member.getKey(), member);
        if (put != null) {
            origin.remove(put);
        }
        return put;
    }

    //ZCARD
    //ZCOUNT
    //ZINCRBY
    public synchronized Member inc(byte[] member, BigDecimal inc) {
        Member mem = unique.get(new HashKey(member));
        if (mem != null) {
            mem = new Member(member, inc.add(mem.getScore()));
        } else {
            mem = new Member(member, inc);
        }
        this.addElement(mem);
        return mem;
    }

    //ZRANGE
    public synchronized List<Member> range(int from, int includeTo) {
        Iterator<Member> its = origin.iterator();
        int max = this.origin.size();
        if (from < 0) {
            from = max + from;
        }
        if (includeTo < 0) {
            includeTo += max;
        }
        return this.range(its, from, includeTo);
    }

    public synchronized List<Member> range(Iterator<Member> its, int from, int includeTo) {
        // Iterator<Member> its = origin.iterator();
        List<Member> list = new ArrayList<>();
        for (int i = 0; its.hasNext() && i <= includeTo; i++) {
            if (i >= from) {
                list.add(its.next());
            }
        }
        return list;
    }

    //ZRANGEBYSCORE
    //ZRANK

    public synchronized int rank(Iterator<Member> its, byte[] member) {
        // Iterator<Member> its = origin.iterator();
        for (int i = 0; its.hasNext(); i++) {
            Member next = its.next();
            if (Arrays.equals(next.getMember(), member)) {
                return i;
            }
        }
        return -1;
    }

    //ZREM
    public synchronized int removeAll(List<HashKey> list) {
        int count = 0;
        for (HashKey m : list) {
            Member member = this.unique.remove(m);
            if (member != null) {
                this.origin.remove(member);
                count++;
            }
        }
        return count;
    }

    //ZREMRANGEBYRANK
    public synchronized int remRangeByRank(int start, int includeStop) {
        Iterator<Member> it = this.origin.iterator();
        if (this.origin.isEmpty()) {
            return 0;
        }
        while (start < 0) {
            start += this.origin.size();
        }
        while (includeStop < 0) {
            includeStop += this.origin.size();
        }
        if (start > includeStop) {
            return 0;
        }
        int count = 0;
        for (int i = 0; it.hasNext(); i++) {
            it.next();
            if (i >= start) {
                if (i <= includeStop) {
                    it.remove();
                    count++;
                } else {
                    break;
                }
            }
        }
        return count;
    }

    //ZREMRANGEBYSCORE
    public synchronized int remRangeByScore(SortedSetOperation.Interval min, SortedSetOperation.Interval max) {
        List<HashKey> rms = this.origin.subSet(Member.MIN(min.val), Member.MAX(max.val))
                .stream()
                .filter(e -> min.filterEqual(e)).filter(e -> min.filterEqual(e))
                .map(Member::getKey).collect(Collectors.toList());
        this.removeAll(rms);
        return rms.size();
    }

    //ZREVRANGE
    public synchronized List<Member> revRange(int from, int to) {
        Iterator<Member> its = origin.descendingSet().iterator();
        return this.range(its, from, to);
    }

    //ZREVRANGEBYSCORE
    //ZREVRANK
    public int revRank(byte[] member) {
        return this.rank(origin.descendingSet().iterator(), member);
    }

    //ZSCORE
    public Optional<Member> member(HashKey key) {
        return Optional.ofNullable(this.unique.get(key));
    }

    //ZUNIONSTORE
    // over write
    public synchronized int unionStore(RedisDatabase rd, SortedSetOperation.UnionStore us) {
        SortedSetData[] ssd = Arrays.stream(us.getHashKeys()).map(e -> rd.get(e))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toArray(SortedSetData[]::new);
        int[] wt = us.getWeights();
        int size = recursiveLock(ssd, 0, () -> {
            Map<HashKey, BigDecimal> map = new HashMap<>();
            for (int i = 0; i < ssd.length; i++) {
                SortedSetData s = ssd[i];
                BigDecimal w = BigDecimal.valueOf(wt[i]);
                s.origin.forEach(e -> {
                    BigDecimal og = map.get(e.getKey());
                    og = us.getType().apply(og, e.getScore().multiply(w));
                    map.put(e.getKey(), og);
                });
            }
            map.forEach((k, v) -> { // over write
                this.addElement(new Member(k.getKey(), v));
            });
            return this.origin.size();
        });
        return size;
    }

    public synchronized <T> T recursiveLock(SortedSetData[] ssd, int index, Supplier<T> fun) {
        if (index < ssd.length) {
            return ssd[index].sync(() -> {// recursive
                return recursiveLock(ssd, index + 1, fun);
            });
        }
        return fun.get();
    }

    //ZINTERSTORE
    // over write !
    public synchronized int interStore(RedisDatabase rd, SortedSetOperation.UnionStore us) {
        SortedSetData[] ssd = Arrays.stream(us.getHashKeys()).map(e -> rd.get(e))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toArray(SortedSetData[]::new);
        int[] wt = us.getWeights();
        Map<HashKey, List<Member>> map = ssd[0].getData().stream().collect(Collectors.groupingBy(Member::getKey));
        int size = recursiveLock(ssd, 0, () -> {
            for (int i = 1; i < ssd.length; i++) {
                SortedSetData s = ssd[i];
                s.getData().forEach(e -> {
                    List<Member> m = map.get(e.getKey());
                    if (m != null) {
                        m.add(e);
                    }
                });
            }
            map.forEach((k, vs) -> {
                if (vs.size() != wt.length) {
                    return;
                }
                BigDecimal val = null;
                for (int i = 0; i < vs.size(); i++) {
                    BigDecimal score = vs.get(i).getScore().multiply(BigDecimal.valueOf(wt[i]));
                    val = us.getType().apply(val, score);
                }
                this.addElement(new Member(k.getKey(), val));// over write !
            });
            return this.origin.size();
        });
        return size;
    }
    //ZSCAN
}
