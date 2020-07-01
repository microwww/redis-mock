package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;

import static org.junit.Assert.*;

import org.junit.Test;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SortedSetOperationTest extends AbstractRedisTest {

    @Test
    public void testZadd() {
        String[] r = Server.random(10);
        long zadd = jedis.zadd(r[0], 100, r[1]);
        assertEquals(1, zadd);
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        zadd = jedis.zadd(r[0], map);
        assertEquals(map.size() - 1, zadd);
    }

    @Test
    public void testZcard() {
        String[] r = Server.random(2);
        long len = jedis.zcard(r[0]);
        assertEquals(0, len);
        jedis.zadd(r[0], 10, r[0]);
        jedis.zadd(r[0], 10, r[1]);
        len = jedis.zcard(r[0]);
        assertEquals(2, len);
    }

    @Test
    public void testZcount() {
        String[] r = Server.random(10);
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i + 1));
        }
        long count = jedis.zadd(r[0], map);
        assertEquals(count, map.size());

        count = jedis.zcount(r[0], 3, 6);
        assertEquals(count, 4);
        count = jedis.zcount(r[0], "(3", "6");
        assertEquals(count, 3);
        count = jedis.zcount(r[0], "3", "(6");
        assertEquals(count, 3);
        count = jedis.zcount(r[0], "(3", "(6");
        assertEquals(count, 2);
    }

    @Test
    public void testZincrby() {
        String[] r = Server.random(2);
        double score = jedis.zincrby(r[0], 10, r[1]);
        assertEquals(10, score, 0.0000001);
        score = jedis.zincrby(r[0], 6.59, r[1]);
        assertEquals(16.59, score, 0.0000001);
    }

    @Test
    public void testZrange() {
        String[] r = Server.random(10);
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i + 1));
        }
        long count = jedis.zadd(r[0], map);
        assertEquals(count, map.size());

        Set<String> zrange = jedis.zrange(r[0], 10, 100);
        assertEquals(0, zrange.size());

        zrange = jedis.zrange(r[0], 0, -1);
        assertEquals(map.size(), zrange.size());

        zrange = jedis.zrange(r[0], -5, -1);
        assertEquals(5, zrange.size());

        Set<Tuple> val = jedis.zrangeWithScores(r[0], 0, -1);
        assertEquals(map.size(), val.size());
        assertEquals(1, val.iterator().next().getScore(), 0.0000000001);
    }

    @Test
    public void testZrangebyscore() {
        String[] r = Server.random(50);
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i % 10));
        }
        long count = jedis.zadd(r[0], map);
        assertEquals(count, map.size());

        Set<String> range = jedis.zrangeByScore(r[0], 5, 6);
        assertEquals(10, range.size());
        range = jedis.zrangeByScore(r[0], 5, 6, 1, 2);
        assertEquals(2, range.size());
        Set<Tuple> rangeScore = jedis.zrangeByScoreWithScores(r[0], 5, 6, 1, 2);
        assertEquals(2, range.size());
    }

    @Test
    public void testZrank() {
        // Jedis jedis = new Jedis("192.168.1.246");
        String[] r = Server.random(50);
        {
            Long zrank = jedis.zrank(r[0], r[1]);
            assertNull(zrank);
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);

        Long zrank = jedis.zrank(r[0], r[1]);
        assertEquals(1, zrank.intValue());

        jedis.zadd(r[1], 10, "");
        jedis.zadd(r[1], 10, "a");
        jedis.zadd(r[1], 10, "b");
        jedis.zadd(r[1], 10, "c");

        Long same = jedis.zrank(r[1], "");
        assertEquals(0, same.intValue());
        same = jedis.zrank(r[1], "a");
        assertEquals(1, same.intValue());
        same = jedis.zrank(r[1], "b");
        assertEquals(2, same.intValue());
    }

    @Test
    public void testZrem() {
        String[] r = Server.random(5);
        {
            Long zrank = jedis.zrem(r[0], r[1]);
            assertEquals(0, zrank.intValue());
        }
        jedis.zadd(r[0], 10, r[1]);
        jedis.zadd(r[0], 10, r[2]);
        Long zrank = jedis.zrem(r[0], r[0], r[1]);
        assertEquals(1, zrank.intValue());
    }

    @Test
    public void testZremrangebyrank() {
        String[] r = Server.random(10);
        {
            Long zrank = jedis.zremrangeByRank(r[0], 1, 5);
            assertEquals(0, zrank.intValue());
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);
        {
            long count = jedis.zremrangeByRank(r[0], 2, 3);
            assertEquals(2, count);
        }
        {
            long count = jedis.zremrangeByRank(r[0], 6, -1); // 0,1,4,5,...,9
            assertEquals(2, count);
        }
        {
            long count = jedis.zremrangeByRank(r[0], -2, -1);
            assertEquals(2, count);
        }
    }

    @Test
    public void testZremrangebyscore() {
        String[] r = Server.random(10);
        {
            Long zrank = jedis.zremrangeByScore(r[0], 1, 5);
            assertEquals(0, zrank.intValue());
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);
        {
            long count = jedis.zremrangeByScore(r[0], "(3", "5");
            assertEquals(2, count);
        }
        {
            long count = jedis.zremrangeByScore(r[0], 6, 7);
            assertEquals(2, count);
        }
        {
            long count = jedis.zremrangeByScore(r[0], "(8", "(9");
            assertEquals(0, count);
        }
        {
            long count = jedis.zremrangeByScore(r[0], "1", "(3");
            assertEquals(2, count);
        }
    }

    @Test
    public void testZrevrange() {
        String[] r = Server.random(10);
        {
            Set<String> range = jedis.zrevrange(r[0], 1, 5);
            assertEquals(0, range.size());
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);
        {
            Set<String> range = jedis.zrevrange(r[0], 1, 5);
            assertEquals(5, range.size());
            assertEquals(r[r.length - 2], range.iterator().next());
        }
        {
            Set<String> range = jedis.zrevrange(r[0], -6, -3);
            assertEquals(4, range.size());
            assertEquals(r[r.length - 6 + 1], range.iterator().next());
        }
        {
            Set<Tuple> range = jedis.zrevrangeWithScores(r[0], -6, -3);
            assertEquals(4, range.size());
            assertEquals(5, range.iterator().next().getScore(), 0.000001);
        }
    }

    @Test
    public void testZrevrangebyscore() {
        String[] r = Server.random(10);
        {
            Set<String> range = jedis.zrevrangeByScore(r[0], 1, 5);
            assertEquals(0, range.size());
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);
        {
            Set<String> range = jedis.zrevrangeByScore(r[0], 4, 1);
            assertEquals(4, range.size());
        }
        {
            Set<String> range = jedis.zrevrangeByScore(r[0], "(4", "(1");
            assertEquals(2, range.size());
        }
        {
            Set<String> range = jedis.zrevrangeByScore(r[0], 400, 100);
            assertEquals(0, range.size());
        }
    }

    @Test
    public void testZrevrank() {
        String[] r = Server.random(10);
        {
            Long rank = jedis.zrevrank(r[0], r[1]);
            assertNull(rank);
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);
        {
            long rank = jedis.zrevrank(r[0], r[1]);
            assertEquals(r.length - 2, rank);
        }
    }

    @Test
    public void testZscore() {
        String[] r = Server.random(10);
        {
            Double rank = jedis.zscore(r[0], r[1]);
            assertNull(rank);
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < r.length; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);
        {
            double score = jedis.zscore(r[0], r[1]);
            assertEquals(1, score, 0.00000000001);
        }
        {
            double score = jedis.zscore(r[0], r[7]);
            assertEquals(7, score, 0.00000000001);
        }
    }

    @Test
    public void testZunionstore() {
        String[] r = Server.random(10);
        {
            long store = jedis.zunionstore(r[0], r[1], r[2]);
            assertEquals(0, store);
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);
        //
        map = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            map.put(r[i + 3], Double.valueOf(i));
        }
        jedis.zadd(r[1], map);
        {
            double size = jedis.zunionstore(r[3], r[0], r[1]);
            assertEquals(8, size, 0.00000000001);
            // 0 1 2 3 4 5 6 7 8 9
            // 0 1 2 3 4
            // - - - 0 1 2 3 4
            assertEquals(3, jedis.zscore(r[3], r[3]), 0.000000000001);
            assertEquals(5, jedis.zscore(r[3], r[4]), 0.000000000001);
            assertEquals(2, jedis.zscore(r[3], r[5]), 0.000000000001);
        }
    }

    public void testStoreFromSortedSet() {
    }

    @Test
    public void testZinterstore() {
        String[] r = Server.random(10);
        {
            long store = jedis.zinterstore(r[0], r[1], r[2]);
            assertEquals(0, store);
        }
        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            map.put(r[i], Double.valueOf(i));
        }
        jedis.zadd(r[0], map);
        map = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            map.put(r[i+3], Double.valueOf(i));
        }
        jedis.zadd(r[1], map);
        // 0 1 2 3 4 5 6 7 8 9
        // 0 1 2 3 4
        // - - - 0 1 2 3 4
        long zscore = jedis.zinterstore(r[3], r[0], r[1]);
        assertEquals(2, zscore, 0.000000000001);

        assertNull(jedis.zscore(r[3], r[2]));
        assertEquals(3, jedis.zscore(r[3], r[3]), 0.000000000001);
        assertEquals(5, jedis.zscore(r[3], r[4]), 0.000000000001);
        assertNull(jedis.zscore(r[3], r[5]));

        ZParams params = new ZParams();
        params.aggregate(ZParams.Aggregate.MAX);
        jedis.zinterstore(r[4], params, r[0], r[1]);
        assertEquals(3, jedis.zscore(r[4], r[3]), 0.000000000001);
        assertEquals(4, jedis.zscore(r[4], r[4]), 0.000000000001);
    }
}