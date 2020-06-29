package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;

import static org.junit.Assert.*;

import org.junit.Test;
import redis.clients.jedis.Tuple;

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
        same = jedis.zrank(r[1], "a"); // TODO :: ERROR
        assertEquals(1, same.intValue());
        same = jedis.zrank(r[1], "b");
        assertEquals(2, same.intValue());
    }

    public void testZrem() {
    }

    public void testZremrangebyrank() {
    }

    public void testZremrangebyscore() {
    }

    public void testZrevrange() {
    }

    public void testZrevrangebyscore() {
    }

    public void testZrevrank() {
    }

    public void testZscore() {
    }

    public void testZunionstore() {
    }

    public void testStoreFromSortedSet() {
    }

    public void testZinterstore() {
    }
}