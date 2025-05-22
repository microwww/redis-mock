package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.Member;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.database.SortedSetData;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.ScanIterator;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;
import com.github.microwww.redis.util.SafeEncoder;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public class SortedSetOperation extends AbstractOperation {

    //ZADD
    public void zadd(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        RequestParams[] args = request.getParams();
        SortedSetData ss = getOrCreate(request);
        Member[] ms = new Member[args.length / 2];
        for (int i = 1, j = 0; i < args.length; i += 2, j++) {
            BigDecimal dec = args[i].byteArray2decimal();
            byte[] ba = args[i + 1].getByteArray();
            ms[j] = new Member(ba, dec);
        }
        int count = ss.addOrReplace(ms);
        request.getOutputProtocol().writer(count);
    }

    //ZCARD
    public void zcard(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        Optional<SortedSetData> ss = getData(request);
        int size = ss.map(e -> e.getData().size()).orElse(0);
        request.getOutputProtocol().writer(size);
    }

    //ZCOUNT
    public void zcount(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        int size = 0;
        if (ss.isPresent()) {
            Interval min = new Interval(args[1].getByteArray());
            Interval max = new Interval(args[2].getByteArray());
            long count = ss.get().getData() //
                    .subSet(Member.MIN(min.val), Member.MAX(max.val)) //
                    .stream() //
                    .filter(e -> min.filterEqual(e)).filter(e -> max.filterEqual(e)).count();
            size = (int) count;
        }
        request.getOutputProtocol().writer(size);
    }

    //ZINCRBY
    public void zincrby(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        SortedSetData ss = getOrCreate(request);
        BigDecimal inc = args[1].byteArray2decimal();
        byte[] val = args[2].getByteArray();
        Member mem = ss.inc(val, inc);
        request.getOutputProtocol().writer(mem.getScore().toPlainString());
    }

    //ZRANGE
    public void zrange(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        List<byte[]> list = new ArrayList<>();
        if (ss.isPresent()) {
            int start = args[1].byteArray2int();
            int stop = args[2].byteArray2int();
            List<Member> mem = ss.get().range(start, stop);
            boolean withScores = args.length == 4;
            for (Member m : mem) {
                list.add(m.getMember());
                if (withScores) {
                    list.add(m.getScore().toPlainString().getBytes());
                }
            }
        }
        request.getOutputProtocol().writerMulti(list.toArray(new byte[list.size()][]));
    }

    //ZRANGEBYSCORE
    public void zrangebyscore(RedisRequest request) throws IOException {
        this.rangeByScore(request, false);
    }

    private void rangeByScore(RedisRequest request, boolean desc) throws IOException {
        request.expectArgumentsCountGE(3);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        List<byte[]> list = new ArrayList<>();
        if (ss.isPresent()) {
            Interval start = new Interval(args[1].getByteArray());
            Interval stop = new Interval(args[2].getByteArray());
            RangeByScore spm = new RangeByScore();
            for (int i = 3; i < args.length; i++) {
                String op = args[i].getByteArray2string();
                RangeByScoreParams pm = RangeByScoreParams.valueOf(op.toUpperCase());
                i = pm.next(spm, args, i);
            }
            AtomicInteger count = new AtomicInteger(0);
            long end = spm.getCount() + spm.getOffset();
            ss.get().getSubSetData(desc, start, stop)
                    .stream()
                    .filter(e -> start.filterEqual(e)).filter(e -> stop.filterEqual(e))
                    .forEach(e -> {
                        int i = count.getAndIncrement();
                        if (i >= end) {
                            return;
                        }
                        if (i >= spm.offset) {
                            list.add(e.getMember());
                            if (spm.withScores) {
                                list.add(e.getScore().toPlainString().getBytes());
                            }
                        }
                    });
        }
        request.getOutputProtocol().writerMulti(list.toArray(new byte[list.size()][]));
    }

    //ZRANK, rank from 0
    public void zrank(RedisRequest request) throws IOException {
        this.rank(request, false);
    }

    private void rank(RedisRequest request, boolean desc) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        if (ss.isPresent()) {
            Optional<Member> member = ss.get().member(args[1].byteArray2hashKey());
            if (member.isPresent()) {
                NavigableSet<Member> members = ss.get().getData();
                if (desc) {
                    members = members.descendingSet();
                }
                int i = members.headSet(member.get()).size();
                request.getOutputProtocol().writer(i);
                return;
            }
        }
        request.getOutputProtocol().writerNull();
    }

    //ZREM
    public void zrem(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        int count = 0;
        if (ss.isPresent()) {
            List<HashKey> list = new ArrayList<>();
            for (int i = 1; i < args.length; i++) {
                list.add(args[i].byteArray2hashKey());
            }
            count = ss.get().removeAll(list);
        }
        request.getOutputProtocol().writer(count);
    }

    //ZREMRANGEBYRANK
    public void zremrangebyrank(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        int count = 0;
        if (ss.isPresent()) {
            count = ss.get().remRangeByRank(args[1].byteArray2int(), args[2].byteArray2int());
        }
        request.getOutputProtocol().writer(count);
    }

    //ZREMRANGEBYSCORE
    public void zremrangebyscore(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        int count = 0;
        if (ss.isPresent()) {
            Interval min = new Interval(args[1].getByteArray());
            Interval max = new Interval(args[2].getByteArray());
            count = ss.get().remRangeByScore(min, max);
        }
        request.getOutputProtocol().writer(count);
    }

    //ZREVRANGE
    public void zrevrange(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        List<byte[]> list = new ArrayList<>();
        if (ss.isPresent()) {
            int start = args[1].byteArray2int();
            int stop = args[2].byteArray2int();
            List<Member> mem = ss.get().revRange(start, stop);
            boolean withScores = args.length == 4;
            for (Member m : mem) {
                list.add(m.getMember());
                if (withScores) {
                    list.add(m.getScore().toPlainString().getBytes());
                }
            }
        }
        request.getOutputProtocol().writerMulti(list.toArray(new byte[list.size()][]));
    }

    //ZREVRANGEBYSCORE
    public void zrevrangebyscore(RedisRequest request) throws IOException {
        this.rangeByScore(request, true);
    }

    //ZREVRANK
    public void zrevrank(RedisRequest request) throws IOException {
        this.rank(request, true);
    }

    //ZSCORE
    public void zscore(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        Optional<SortedSetData> ss = getData(request);
        if (ss.isPresent()) {
            Optional<Member> member = ss.get().member(args[1].byteArray2hashKey());
            if (member.isPresent()) {
                request.getOutputProtocol().writer(member.get().getScore().toPlainString());
                return;
            }
        }
        request.getOutputProtocol().writerNull();
    }

    //ZUNIONSTORE
    public void zunionstore(RedisRequest request) throws IOException {
        SortedSetData target = this.getOrCreate(request);
        this.storeFromSortedSet(request, (db, param) -> target.unionStore(request.getDatabase(), param));
    }

    public void storeFromSortedSet(RedisRequest request, BiFunction<RedisDatabase, UnionStore, Integer> fun) throws IOException {
        request.expectArgumentsCountGE(3);
        RequestParams[] args = request.getParams();
        int i = 1;
        int num = args[i++].byteArray2int();
        Assert.isTrue(num + i <= args.length, "num-keys count error");
        HashKey[] hks = new HashKey[num];
        for (int j = 0; j < num; j++, i++) {
            hks[j] = args[i].byteArray2hashKey();
        }
        UnionStore us = new UnionStore(hks);
        for (; i < args.length; i++) {
            String op = args[i].getByteArray2string();
            i = UnionStoreParam.valueOf(op.toUpperCase()).next(us, args, i);
        }
        int count = fun.apply(request.getDatabase(), us); // 并或交集
        request.getOutputProtocol().writer(count);
    }

    //ZINTERSTORE
    public void zinterstore(RedisRequest request) throws IOException {
        SortedSetData target = this.getOrCreate(request);
        this.storeFromSortedSet(request, (db, param) -> target.interStore(request.getDatabase(), param));
    }

    //ZSCAN
    public void zscan(RedisRequest request) throws IOException {
        Optional<SortedSetData> opt = this.getData(request);
        NavigableSet<Member> hk = opt.map(e -> e.getData()).orElse(Collections.emptyNavigableSet());
        Iterator<Member> iterator = hk.iterator();
        new ScanIterator<Member>(request, 1)
                .skip(iterator)
                .continueWrite(iterator, e -> {// key
                    return e.getMember();
                }, e -> {// value
                    return e.getScore().toPlainString().getBytes();
                });
    }

    @NotNull
    private SortedSetData getOrCreate(RedisRequest request) {
        HashKey key = new HashKey(request.getParams()[0].getByteArray());
        return request.getDatabase().getOrCreate(key, SortedSetData::new);
    }

    private Optional<SortedSetData> getData(RedisRequest request) {
        HashKey hk = request.getParams()[0].byteArray2hashKey();
        return request.getDatabase().get(hk, SortedSetData.class);
    }

    public static class Interval {
        public final boolean open; // open interval
        public final BigDecimal val;

        public Interval(byte[] bytes) {
            String code = SafeEncoder.encode(bytes).trim();
            open = code.startsWith("(");
            if (open) {
                code = code.substring(1);
            }
            val = new BigDecimal(code);
        }

        public boolean filterEqual(Member e) {
            if (this.open) {// open , remove equal
                return this.val.compareTo(e.getScore()) != 0;
            }
            return true;
        }
    }

    public static class RangeByScore {
        private boolean withScores = false;
        private int offset = 0;
        private int count = Integer.MAX_VALUE;

        public boolean isWithScores() {
            return withScores;
        }

        public void setWithScores(boolean withScores) {
            this.withScores = withScores;
        }

        public int getOffset() {
            return offset;
        }

        public void setOffset(int offset) {
            Assert.isTrue(offset >= 0, "Offset >= 0");
            this.offset = offset;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            Assert.isTrue(count > 0, "count > 0");
            this.count = count;
        }
    }

    public enum RangeByScoreParams {
        WITHSCORES {
            @Override
            public int next(RangeByScore params, RequestParams[] args, int i) {
                params.withScores = true;
                return i + 1;
            }
        },
        LIMIT {
            @Override
            public int next(RangeByScore params, RequestParams[] args, int i) {
                params.offset = args[i + 1].byteArray2int();
                params.count = args[i + 2].byteArray2int();
                return i + 2;
            }
        };

        public abstract int next(RangeByScore params, RequestParams[] args, int i);
    }

    public static class UnionStore {
        private final HashKey[] hashKeys;
        private int[] weights;
        private Aggregate type = Aggregate.SUM;

        public UnionStore(HashKey[] hashKeys) {
            this.hashKeys = hashKeys;
            this.weights = new int[this.getHashKeys().length];
            Arrays.fill(this.weights, 1);
        }

        public HashKey[] getHashKeys() {
            return hashKeys;
        }

        public int[] getWeights() {
            return weights;
        }

        public void setWeights(int[] weights) {
            this.weights = weights;
        }

        public Aggregate getType() {
            return type;
        }

        public void setType(Aggregate type) {
            this.type = type;
        }
    }

    public enum UnionStoreParam {
        WEIGHTS {
            @Override
            public int next(UnionStore params, RequestParams[] args, int i) {
                Assert.isTrue(args.length > i + params.getHashKeys().length, "WEIGHTS  count error");
                int[] w = new int[params.getHashKeys().length];
                for (int j = 0; j < params.getHashKeys().length; j++, i++) {
                    w[j] = args[i + j + 1].byteArray2int();
                }
                params.setWeights(w);
                return i + w.length;
            }
        },
        AGGREGATE {
            @Override
            public int next(UnionStore params, RequestParams[] args, int i) {
                Aggregate agg = Aggregate.valueOf(args[i + 1].getByteArray2string().toUpperCase());
                params.setType(agg);
                return i + 1;
            }
        };

        public abstract int next(UnionStore params, RequestParams[] args, int i);
    }

    public enum Aggregate implements BinaryOperator<BigDecimal> {
        SUM {
            @Override
            public BigDecimal apply(BigDecimal d1, BigDecimal d2) {
                Assert.isNotNull(d2, "Not null");
                if (d1 == null) {
                    return d2;
                }
                return d1.add(d2);
            }
        },
        MIN {
            @Override
            public BigDecimal apply(BigDecimal d1, BigDecimal d2) {
                Assert.isNotNull(d2, "Not null");
                if (d1 == null) {
                    return d2;
                }
                return d1.compareTo(d2) > 0 ? d2 : d1;
            }
        },
        MAX {
            @Override
            public BigDecimal apply(BigDecimal d1, BigDecimal d2) {
                Assert.isNotNull(d2, "Not null");
                if (d1 == null) {
                    return d2;
                }
                return d1.compareTo(d2) > 0 ? d1 : d2;
            }
        }
    }
}
