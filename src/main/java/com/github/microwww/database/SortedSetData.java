package com.github.microwww.database;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class SortedSetData extends AbstractValueData<Set<SortedSetData.Member>> {
    private final ConcurrentSkipListSet<SortedSetData.Member> origin;

    public static class Member implements Comparable {
        private final byte[] member;
        private final double score;

        public Member(byte[] member, double score) {
            this.member = member;
            this.score = score;
        }

        public byte[] getMember() {
            byte[] bt = new byte[this.member.length];
            System.arraycopy(this.member, 0, bt, 0, bt.length);
            return bt;
        }

        public double getScore() {
            return score;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return this.compareTo(o) == 0;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(member);
        }

        @Override
        public int compareTo(Object o) {
            if (!(o instanceof Member)) {
                throw new UnsupportedOperationException();
            }
            Member o1 = this;
            Member o2 = (Member) o;
            int c = Double.compare(o1.score, o2.score);
            if (c == 0) {
                return ByteData.COMPARATOR.compare(o1.member, o2.member);
            }
            return c;
        }
    }

    public SortedSetData() {
        this(NEVER_EXPIRE);
    }

    public SortedSetData(int exp) {
        this.origin = new ConcurrentSkipListSet<>();
        this.data = Collections.unmodifiableSet(this.origin);
        this.expire = exp;
    }

    //ZADD
    public synchronized boolean add(Member member) {
        return origin.add(member);
    }

    //ZCARD
    //ZCOUNT
    //ZINCRBY
    public synchronized Member inc(byte[] member, double inc) {
        Optional<Member> mb = this.find(member);
        Member m;
        if (mb.isPresent()) {
            Member o = mb.get();
            this.origin.remove(o);
            m = new Member(member, inc + o.score);
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
            if (Arrays.equals(next.member, member)) {
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
    public synchronized List<Member> rangeByScore(double minScore, double maxScore, int off, int count) {
        Member mb = new Member("".getBytes(), minScore);
        Iterator<Member> its = origin.tailSet(mb).iterator();
        return this.rangeByScore(its, minScore, maxScore, off, count);
    }

    public synchronized List<Member> rangeByScore(Iterator<Member> its, double minScore, double maxScore, int off, int count) {
        List<Member> list = new ArrayList<>();
        int max = off + count;
        for (int i = off; its.hasNext() && i < max; i++) {
            Member next = its.next();
            if (next.score >= minScore) {
                list.add(next);
                if (next.score < maxScore) {
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
    public synchronized List<Member> revRangeByScore(double minScore, double maxScore, int off, int count) {
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
