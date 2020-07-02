package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;

import static org.junit.Assert.*;

import org.junit.Test;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Set;

public class SetOperationTest extends AbstractRedisTest {

    @Test
    public void testSadd() {
        String[] r = Server.random(5);
        long sadd = jedis.sadd(r[0], r[1], r[1], r[2], r[3]);
        assertEquals(3, sadd);
    }

    @Test
    public void testScard() {
        String[] r = Server.random(5);
        long scard = jedis.scard(r[0]);
        assertEquals(0, scard);
        jedis.sadd(r[0], r[1], r[1], r[2], r[3]);
        scard = jedis.scard(r[0]);
        assertEquals(3, scard);
    }

    @Test
    public void testSdiff() {
        String[] r = Server.random(10);
        Set<String> scard = jedis.sdiff(r[0], r[1]);
        assertTrue(scard.isEmpty());

        // 0 1 2 3 4
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[0], r[i]);
        }
        scard = jedis.sdiff(r[0], r[1]);
        assertEquals(5, scard.size());

        //     2 3 4 5 6
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[1], r[i + 2]);
        }
        Set<String> sdiff = jedis.sdiff(r[0], r[1]);
        assertEquals(2, sdiff.size());
        assertTrue(sdiff.contains(r[0]));
        assertTrue(sdiff.contains(r[1]));

        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[1], r[i + 2]);
        }
        sdiff = jedis.sdiff(r[0], r[1], r[2]);
        assertEquals(2, sdiff.size());
    }

    @Test
    public void testSdiffstore() {
        String[] r = Server.random(10);
        long scard = jedis.sdiffstore(r[9], r[0], r[1]);
        assertEquals(0, scard);

        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[0], r[i]);
        }
        scard = jedis.sdiffstore(r[9], r[0], r[1]);
        assertEquals(5, scard);

        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[1], r[i + 3]);
        }
        // 0 1 2 3 4
        //       3 4 5 6 7
        scard = jedis.sdiffstore(r[9], r[0], r[1]);
        assertEquals(3, scard);
        assertEquals(3, jedis.scard(r[9]).intValue());
    }

    @Test
    public void testSinter() {
        String[] r = Server.random(10);
        Set<String> scard = jedis.sinter(r[0], r[1]);
        assertTrue(scard.isEmpty());

        // 0 1 2 3 4
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[0], r[i]);
        }
        scard = jedis.sinter(r[0], r[1]);
        assertEquals(0, scard.size());

        // - - 2 3 4 5 6
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[1], r[i + 2]);
        }

        scard = jedis.sinter(r[0], r[1]);
        assertEquals(3, scard.size());

        // - - - - 4 5 6 7 8
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[2], r[i + 4]);
        }

        scard = jedis.sinter(r[0], r[1], r[2]);
        assertEquals(1, scard.size());
        assertTrue(scard.contains(r[4]));
    }

    @Test
    public void testSinterstore() {
        String[] r = Server.random(10);
        String target = r[9];
        long scard = jedis.sinterstore(target, r[0], r[1]);
        assertEquals(0, scard);

        // 0 1 2 3 4
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[0], r[i]);
        }
        scard = jedis.sinterstore(target, r[0], r[1]);
        assertEquals(0, scard);

        //     2 3 4 5 6
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[1], r[i + 2]);
        }
        {
            scard = jedis.sinterstore(target, r[0], r[1]);
            assertEquals(3, scard);
            assertEquals(3, jedis.scard(target).intValue());
            Set<String> ms = jedis.smembers(target);
            assertTrue(ms.contains(r[3]));
            assertTrue(ms.contains(r[4]));
        }
        //         4 5 6 7 8
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[2], r[i + 4]);
        }
        {
            scard = jedis.sinterstore(target, r[0], r[1], r[2]);
            assertEquals(1, scard);
            Set<String> ms = jedis.smembers(target);
            assertTrue(ms.contains(r[4]));
            scard = jedis.sinterstore(target, r[0]); // over write
            assertEquals(5, scard);
            assertEquals(5, jedis.scard(target).intValue());
        }
    }

    @Test
    public void testSismember() {
        String[] r = Server.random(5);
        boolean exist = jedis.sismember(r[0], r[1]);
        assertFalse(exist);

        jedis.sadd(r[0], r[1], r[2], r[3]);
        exist = jedis.sismember(r[0], r[1]);
        assertTrue(exist);
        assertFalse(jedis.sismember(r[0], r[4]));
    }

    @Test
    public void testSmembers() {
        String[] r = Server.random(5);
        {
            Set<String> meb = jedis.smembers(r[0]);
            assertTrue(meb.isEmpty());
        }
        jedis.sadd(r[1], r[0], r[1], r[2], r[3], r[4]);
        {
            Set<String> meb = jedis.smembers(r[1]);
            assertTrue(meb.contains(r[0]));
            assertTrue(meb.contains(r[2]));
            assertTrue(meb.contains(r[3]));
            assertTrue(meb.contains(r[4]));
        }
    }

    @Test
    public void testSmove() {
        String[] r = Server.random(5);
        {
            long meb = jedis.smove(r[0], r[1], r[2]);
            assertEquals(0, meb);
        }
        jedis.sadd(r[0], r[0], r[1], r[2], r[3], r[4]);
        {
            long meb = jedis.smove(r[0], r[1], r[2]);
            assertEquals(1, meb);
            meb = jedis.smove(r[0], r[1], r[2]); // try again
            assertEquals(0, meb);
        }
        jedis.sadd(r[1], r[3], r[4]);
        {
            long meb = jedis.smove(r[0], r[1], r[3]);
            assertEquals(1, meb);
            assertFalse(jedis.smembers(r[0]).contains(r[3]));
            assertTrue(jedis.smembers(r[1]).contains(r[3]));
        }

    }

    @Test
    public void testSpop() {
        String[] r = Server.random(5);
        {
            String meb = jedis.spop(r[0]);
            assertNull(meb);
            Set<String> spop = jedis.spop(r[0], 2);
            assertTrue(spop.isEmpty());
        }
        jedis.sadd(r[0], r[0], r[1], r[2], r[3], r[4]);
        {
            String meb = jedis.spop(r[0]);
            assertNotNull(meb);
            long scard = jedis.scard(r[0]);
            assertEquals(4, scard);
            Set<String> spop = jedis.spop(r[0], 2);
            assertEquals(2, spop.size());
            scard = jedis.scard(r[0]);
            assertEquals(2, scard);
        }
    }

    @Test
    public void testSrandmember() {
        String[] r = Server.random(10);
        {
            String meb = jedis.srandmember(r[0]);
            assertNull(meb);
            List<String> spop = jedis.srandmember(r[0], 2);
            assertTrue(spop.isEmpty());
        }
        jedis.sadd(r[0], r[0], r[1], r[2], r[3], r[4], r[5], r[6]);
        {
            String meb = jedis.srandmember(r[0]);
            assertNotNull(meb);
            long scard = jedis.scard(r[0]);
            assertEquals(7, scard);
            List<String> spop = jedis.srandmember(r[0], 2);
            assertEquals(2, spop.size());
            scard = jedis.scard(r[0]);
            assertEquals(7, scard);
        }
    }

    @Test
    public void testSrem() {
        String[] r = Server.random(10);
        {
            long meb = jedis.srem(r[0], r[1]);
            assertEquals(0, meb);
            long spop = jedis.srem(r[0], r[1], r[2], r[3]);
            assertEquals(0, spop);
        }
        jedis.sadd(r[0], r[0], r[1], r[2], r[3], r[4], r[5], r[6]);
        {
            long meb = jedis.srem(r[0], r[1], r[2]);
            assertEquals(2, meb);
            long scard = jedis.scard(r[0]);
            assertEquals(5, scard);
            meb = jedis.srem(r[0], r[4], r[8]);
            assertEquals(1, meb);
            scard = jedis.scard(r[0]);
            assertEquals(4, scard);

            meb = jedis.srem(r[0], r[7], r[9]);
            assertEquals(0, meb);
            scard = jedis.scard(r[0]);
            assertEquals(4, scard);
        }
    }

    @Test
    public void testSunion() {
        String[] r = Server.random(15);
        {
            Set<String> meb = jedis.sunion(r[0], r[1]);
            assertEquals(0, meb.size());
        }
        // 0 1 2 3 4
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[0], r[i]);
        }
        //       3 4 5 6 7
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[1], r[i + 3]);
        }
        //             6 7 8 9 A
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[2], r[i + 6]);
        }
        {
            Set<String> meb = jedis.sunion(r[0], r[1]);
            assertEquals(8, meb.size());
            assertTrue(meb.contains(r[5]));
        }
        {
            Set<String> meb = jedis.sunion(r[0], r[1], r[3]);
            assertEquals(8, meb.size());
        }
        {
            Set<String> meb = jedis.sunion(r[0], r[1], r[2]);
            assertEquals(11, meb.size());
            assertTrue(meb.contains(r[10]));
        }
    }

    @Test
    public void testSunionstore() {
        String[] r = Server.random(15);
        {
            long meb = jedis.sunionstore(r[0], r[1]);
            assertEquals(0, meb);
            meb = jedis.sunionstore(r[0], r[1], r[2]);
            assertEquals(0, meb);
            assertEquals(0, jedis.scard(r[0]).intValue());
        }
        // 0 1 2 3 4
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[0], r[i]);
        }
        //       3 4 5 6 7
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[1], r[i + 3]);
        }
        //             6 7 8 9 A
        for (int i = 0; i < 5; i++) {
            jedis.sadd(r[2], r[i + 6]);
        }
        {
            long meb = jedis.sunionstore(r[8], r[1], r[3]);
            assertEquals(5, meb);
            assertEquals(5, jedis.scard(r[8]).intValue());

        }
        {
            long meb = jedis.sunionstore(r[9], r[0]);
            assertEquals(5, meb);
            assertEquals(5, jedis.scard(r[9]).intValue());
        }
        {
            long meb = jedis.sunionstore(r[9], r[1], r[2], r[3]);
            assertEquals(8, meb);
            assertEquals(8, jedis.scard(r[9]).intValue());
            assertTrue(jedis.smembers(r[9]).contains(r[7]));
            assertFalse(jedis.smembers(r[9]).contains(r[1]));
        }
        {
            jedis.expire(r[9], 10);
            long meb = jedis.sunionstore(r[9], r[2]);
            assertTrue(jedis.ttl(r[9]) < 0);
            assertEquals(5, meb);
            assertEquals(5, jedis.scard(r[9]).intValue());
            assertTrue(jedis.smembers(r[9]).contains(r[7]));
        }
    }

    @Test(timeout = 3000)
    public void testSscan() {
        String[] r = Server.random(25);
        jedis.select(7);
        for (int i = 0; i < r.length; i++) {
            jedis.sadd(r[0], r[i]);
        }
        String cursor = "0";
        int size = 0;
        while (true) {
            ScanResult<String> scan = jedis.sscan(r[0], cursor);
            cursor = scan.getStringCursor();
            size += scan.getResult().size();
            if ("0".equalsIgnoreCase(cursor)) {
                break;
            }
        }
        assertEquals(25, size);
    }
}