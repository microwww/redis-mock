package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.*;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.util.NotNull;
import redis.clients.util.SafeEncoder;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class SortedSetOperation extends AbstractOperation {

    //ZADD
    public void zadd(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        ExpectRedisRequest[] args = request.getArgs();
        SortedSetData ss = getOrCreate(request);
        Member[] ms = new Member[args.length / 2];
        for (int i = 1; i < args.length; i += 2) {
            BigDecimal dec = args[i].byteArray2decimal();
            byte[] ba = args[i].getByteArray();
            ms[i - 1] = new Member(ba, dec);
        }
        int count = ss.addOrReplace(ms);
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //ZCARD
    public void zcard(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        Optional<SortedSetData> ss = getData(request);
        int size = ss.map(e -> e.getData().size()).orElse(0);
        RedisOutputProtocol.writer(request.getOutputStream(), size);
    }

    //ZCOUNT
    public void zcount(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<SortedSetData> ss = getData(request);
        int size = 0;
        if (ss.isPresent()) {
            Interval min = new Interval(args[1].getByteArray());
            Interval max = new Interval(args[2].getByteArray());
            long count = ss.get().getData() //
                    .subSet(Member.MIN(min.val), Member.MAX(max.val)) //
                    .stream() //
                    .filter(e -> filterEqual(e, min)).filter(e -> filterEqual(e, max)).count();
            size = (int) count;
        }
        RedisOutputProtocol.writer(request.getOutputStream(), size);
    }

    private static boolean filterEqual(Member e, Interval min) {
        if (min.open) {// open , remove equal
            return !min.val.equals(e.getScore());
        }
        return true;
    }

    //ZINCRBY
    public void zincrby(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        SortedSetData ss = getOrCreate(request);
        BigDecimal inc = args[1].byteArray2decimal();
        byte[] val = args[2].getByteArray();
        Member mem = ss.inc(val, inc);
        RedisOutputProtocol.writer(request.getOutputStream(), mem.getScore().toPlainString());
    }

    //ZRANGE
    public void zrange(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        ExpectRedisRequest[] args = request.getArgs();
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
        RedisOutputProtocol.writerMulti(request.getOutputStream(), list.toArray(new byte[list.size()][]));
    }

    //ZRANGEBYSCORE
    public void zrangebyscore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<SortedSetData> ss = getData(request);
        List<byte[]> list = new ArrayList<>();
        if (ss.isPresent()) {
            Interval min = new Interval(args[1].getByteArray());
            Interval max = new Interval(args[2].getByteArray());
            RangeByScore spm = new RangeByScore();
            for (int i = 3; i < args.length; i++) {
                String op = args[i].getByteArray2string();
                RangeByScoreParams pm = RangeByScoreParams.valueOf(op.toUpperCase());
                i = pm.next(spm, args, i);
            }
            ss.get().getData().subSet(Member.MIN(min.val), Member.MAX(max.val))
                    .stream()
                    .filter(e -> filterEqual(e, min)).filter(e -> filterEqual(e, max))
                    .forEach(e -> {
                        list.add(e.getMember());
                        if (spm.withScores) {
                            list.add(e.getScore().toPlainString().getBytes());
                        }
                    });
        }
        RedisOutputProtocol.writerMulti(request.getOutputStream(), list.toArray(new byte[list.size()][]));
    }

    //ZRANK, rank from 0
    public void zrank(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<SortedSetData> ss = getData(request);
        if (ss.isPresent()) {
            Optional<Member> member = ss.get().member(args[1].byteArray2hashKey());
            if (member.isPresent()) {
                int i = ss.get().getData().headSet(Member.MIN(member.get().getScore())).size();
                RedisOutputProtocol.writer(request.getOutputStream(), i);
            }
        }
        RedisOutputProtocol.writerNull(request.getOutputStream());
    }

    //ZREM
    public void zrem(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<SortedSetData> ss = getData(request);
        int count = 0;
        if (ss.isPresent()) {
            List<HashKey> list = new ArrayList<>();
            for (int i = 1; i < args.length; i++) {
                list.add(args[i].byteArray2hashKey());
            }
            count = ss.get().removeAll(list);
        }
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //ZREMRANGEBYRANK
    public void zremrangebyrank(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<SortedSetData> ss = getData(request);
        int count = 0;
        if (ss.isPresent()) {
            count = ss.get().remRangeByRank(args[1].byteArray2int(), args[2].byteArray2int());
        }
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }
    //ZREMRANGEBYSCORE
    //ZREVRANGE
    //ZREVRANGEBYSCORE
    //ZREVRANK
    //ZSCORE
    //ZUNIONSTORE
    //ZINTERSTORE
    //ZSCAN

    @NotNull
    private SortedSetData getOrCreate(RedisRequest request) {
        HashKey key = new HashKey(request.getArgs()[0].getByteArray());
        return request.getDatabase().getOrCreate(key, SortedSetData::new);
    }

    private Optional<SortedSetData> getData(RedisRequest request) {
        HashKey hk = request.getArgs()[0].byteArray2hashKey();
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
            this.offset = offset;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    public enum RangeByScoreParams {
        WITHSCORES {
            @Override
            public int next(RangeByScore params, ExpectRedisRequest[] args, int i) {
                params.withScores = true;
                return i + 1;
            }
        },
        LIMIT {
            @Override
            public int next(RangeByScore params, ExpectRedisRequest[] args, int i) {
                params.offset = args[i + 1].byteArray2int();
                params.count = args[i + 2].byteArray2int();
                return i + 2;
            }
        };

        public abstract int next(RangeByScore params, ExpectRedisRequest[] args, int i);
    }
}
