package com.github.microwww.redis.protocal.message;

import com.github.microwww.redis.protocal.HalfPackException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TypeTest {

    @Test
    public void test0() {
        byte[] bytes = "*1\r\n$1\r\nA\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        MultiMessage rm = (MultiMessage) Type.parseOne(bf);
        Assert.assertEquals('*', rm.type.prefix);
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test1() {
        byte[] bytes = "*2\r\n*2\r\n:1\r\n:2\r\n#t\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(MultiMessage.class.isInstance(rm));
        Assert.assertEquals('*', rm.type.prefix);
        Assert.assertEquals('*', rm.getRedisMessages()[0].type.prefix);
        Assert.assertTrue(LongMessage.class.isInstance(rm.getRedisMessages()[0].getRedisMessages()[0]));
        Assert.assertTrue(BooleanMessage.class.isInstance(rm.getRedisMessages()[1]));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test2() {
        byte[] bytes = "$11\r\nhello world\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(StringMessage.class.isInstance(rm));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test3() {
        byte[] bytes = "+string\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(StringMessage.class.isInstance(rm));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test4() {
        byte[] bytes = "-ERR this is the error description\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(ErrorMessage.class.isInstance(rm));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test5() {
        byte[] bytes = ",10\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();
        {
            RedisMessage rm = Type.parseOne(bf);
            Assert.assertTrue(DoubleMessage.class.isInstance(rm));
        }
        bf.clear();
        bytes = ",inf\r\n".getBytes(StandardCharsets.UTF_8);
        bf.put(bytes);
        bf.flip();
        {
            RedisMessage rm = Type.parseOne(bf);
            Assert.assertTrue(DoubleMessage.class.isInstance(rm));
        }
        bf.clear();
        bytes = ",-inf\r\n".getBytes(StandardCharsets.UTF_8);
        bf.put(bytes);
        bf.flip();
        {
            RedisMessage rm = Type.parseOne(bf);
            Assert.assertTrue(DoubleMessage.class.isInstance(rm));
        }
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test6() {
        byte[] bytes = "!21\r\nSYNTAX invalid syntax\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(ErrorMessage.class.isInstance(rm));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test7() {
        byte[] bytes = "=15\r\ntxt:Some string\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(VerbatimMessage.class.isInstance(rm));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test8() {
        byte[] bytes = "(3492890328409238509324850943850943825024385\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(BigIntMessage.class.isInstance(rm));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test9() {
        byte[] bytes = "*3\r\n:1\r\n:2\r\n:3\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(LongMessage.class.isInstance(rm.getRedisMessages()[0]));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test10() {
        byte[] bytes = "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(MapMessage.class.isInstance(rm));

        Map<String, RedisMessage> map = ((MapMessage) rm).mapString();
        Assert.assertTrue(LongMessage.class.isInstance(map.get("first")));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test11() {
        byte[] bytes = "*3\r\n:1\r\n:2\r\n:3\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(LongMessage.class.isInstance(rm.getRedisMessages()[0]));
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test12() {
        byte[] bytes = "~5\r\n+orange\r\n+apple\r\n#t\r\n:100\r\n:999\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(SetsMessage.class.isInstance(rm));
        Assert.assertTrue(StringMessage.class.isInstance(rm.getRedisMessages()[0]));
        Assert.assertEquals("orange", rm.getRedisMessages()[0].toString());
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test13() {
        byte[] bytes = "$?\r\n;4\r\nHell\r\n;5\r\no wor\r\n;1\r\nd\r\n;0\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();

        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(StringMessage.class.isInstance(rm));
        Assert.assertEquals("Hello word", rm.getRedisMessages()[0].toString());
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test14() {
        byte[] bytes = "|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n:100\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();
        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(LongMessage.class.isInstance(rm));
        Map<String, RedisMessage> map = rm.getAttr().mapString();
        Map<String, RedisMessage> m2 = ((MapMessage) map.get("key-popularity")).mapString();
        Assert.assertEquals("0.1923", m2.get("a").toString());
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void test15() {
        byte[] bytes = "*3\r\n:1\r\n:2\r\n|1\r\n+ttl\r\n:3600\r\n:3\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put(bytes);
        bf.flip();
        RedisMessage rm = Type.parseOne(bf);
        Assert.assertTrue(MultiMessage.class.isInstance(rm));
        Map<String, RedisMessage> map = rm.getRedisMessages()[2].getAttr().mapString();
        Assert.assertEquals("3600", map.get("ttl").toString());
        Assert.assertEquals(bytes.length, bf.position());
    }

    @Test
    public void testHalfPackage() {
        byte[] bytes = ("$?\r\n;4\r\nHell\r\n;5\r\no wor\r\n;1\r\nd\r\n;0\r\n" +
                "|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n:100\r\n").getBytes(StandardCharsets.UTF_8);
        ByteBuffer bf = ByteBuffer.allocate(1024);

        boolean pass = false;
        for (int i = 0; i <= bytes.length; i++) {
            bf.clear();
            bf.put(bytes, 0, i);
            bf.flip();
            try {
                RedisMessage rm = Type.parseOne(bf);
                Assert.assertTrue(StringMessage.class.isInstance(rm));
                rm = Type.parseOne(bf);
                Assert.assertTrue(LongMessage.class.isInstance(rm));
                Assert.assertEquals(bf.remaining(), 0);
                pass = true;
            } catch (HalfPackException ex) {
            }
        }
        Assert.assertTrue(pass);
    }
}