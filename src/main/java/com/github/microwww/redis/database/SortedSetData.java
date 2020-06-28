package com.github.microwww.redis.database;

import com.github.microwww.redis.util.Assert;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class SortedSetData extends AbstractValueData<SortedSet<Member>> {
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
        for (int i = from; its.hasNext() && i <= includeTo; i++) {
            list.add(its.next());
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
    //ZINTERSTORE
    //ZSCAN
}
