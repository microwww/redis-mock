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
    public synchronized int add(Member... member) {
        int count = 0;
        for (Member m : member) {
            Member og = this.addElement(m);
            if (og == null) {
                count++;
            }
        }
        return count;
    }

    public synchronized Member addElement(Member member) {
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
        Optional<Member> mb = this.find(member);
        Member m;
        if (mb.isPresent()) {
            Member o = mb.get();
            this.origin.remove(o);
            m = new Member(member, inc.add(o.getScore()));
        } else {
            m = new Member(member, inc);
        }
        this.origin.add(m);
        return m;
    }

    public synchronized Optional<Member> find(byte[] member) {
        Iterator<Member> it = origin.iterator();
        while (it.hasNext()) {
            Member next = it.next();
            if (Arrays.equals(next.getMember(), member)) {
                return Optional.of(next);
            }
        }
        return Optional.empty();
    }

    //ZRANGE
    public synchronized List<Member> range(int from, int to) {
        Iterator<Member> its = origin.iterator();
        return this.range(its, from, to);
    }

    public synchronized List<Member> range(Iterator<Member> its, int from, int to) {
        // Iterator<Member> its = origin.iterator();
        List<Member> list = new ArrayList<>();
        for (int i = from; its.hasNext() && i < to; i++) {
            list.add(its.next());
        }
        return list;
    }

    /**
     * min_score.score <= member <= max_score.score
     *
     * @param minScore
     * @param maxScore
     * @return
     */
    //ZRANGEBYSCORE
    public synchronized List<Member> rangeByScore(BigDecimal minScore, BigDecimal maxScore, int off, int count) {
        Member mb = new Member("".getBytes(), minScore);
        Iterator<Member> its = origin.tailSet(mb).iterator();
        return this.rangeByScore(its, minScore, maxScore, off, count);
    }

    public synchronized List<Member> rangeByScore(Iterator<Member> its, BigDecimal minScore, BigDecimal maxScore, int off, int count) {
        List<Member> list = new ArrayList<>();
        int max = off + count;
        for (int i = off; its.hasNext() && i < max; i++) {
            Member next = its.next();
            if (next.scoreGE(minScore)) {
                list.add(next);
                if (next.scoreLT(maxScore)) {
                    break;
                }
            }
        }
        return list;
    }

    /**
     * from 0
     *
     * @param member
     * @return
     */
    //ZRANK
    public int rank(byte[] member) {
        return this.rank(origin.iterator(), member);
    }

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
    public Optional<Member> remove(byte[] o) {
        Iterator<Member> its = this.origin.iterator();
        while (its.hasNext()) {
            Member e = its.next();
            if (Arrays.equals(e.getMember(), o)) {
                its.remove();
                return Optional.of(e);
            }
        }
        return Optional.empty();
    }

    //ZREMRANGEBYRANK
    //ZREMRANGEBYSCORE
    //ZREVRANGE
    public synchronized List<Member> revRange(int from, int to) {
        Iterator<Member> its = origin.descendingSet().iterator();
        return this.range(its, from, to);
    }

    //ZREVRANGEBYSCORE
    public synchronized List<Member> revRangeByScore(BigDecimal minScore, BigDecimal maxScore, int off, int count) {
        Member mb = new Member("".getBytes(), minScore);
        Iterator<Member> its = origin.descendingSet().tailSet(mb).iterator();
        return this.rangeByScore(its, minScore, maxScore, off, count);
    }

    //ZREVRANK
    public int revRank(byte[] member) {
        return this.rank(origin.descendingSet().iterator(), member);
    }
    //ZSCORE
    //ZUNIONSTORE
    //ZINTERSTORE
    //ZSCAN
}
