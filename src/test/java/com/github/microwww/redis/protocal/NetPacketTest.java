package com.github.microwww.redis.protocal;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class NetPacketTest {

    @Test
    public void parseSTATUS() {
        String v = "+OK\r\r\n";
        ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
        for (int i = 1; i <= v.length(); i++) {
            buffer.clear();
            buffer.put(v.substring(0, i).getBytes(StandardCharsets.UTF_8));
            buffer.flip();
            Assert.assertEquals('+', buffer.get());
            Optional<? extends NetPacket> parse = NetPacket.Type.STATUS.read(buffer);
            if (i != v.length()) {
                Assert.assertFalse(parse.isPresent());
            } else {
                Assert.assertTrue(parse.isPresent());
            }
        }
    }

    @Test
    public void parseMULTI() {
        String v = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
        ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
        for (int i = 1; i <= v.length(); i++) {
            buffer.clear();
            buffer.put(v.substring(0, i).getBytes(StandardCharsets.UTF_8));
            buffer.flip();
            Assert.assertEquals('*', buffer.get());
            Optional<? extends NetPacket> parse = NetPacket.Type.MULTI.read(buffer);
            if (i != v.length()) {
                Assert.assertFalse(parse.isPresent());
            } else {
                Assert.assertTrue(parse.isPresent());
            }
        }
        {
            buffer.clear();
            buffer.put("*-1\r\n".getBytes(StandardCharsets.UTF_8)).flip();
            Assert.assertEquals(NetPacket.parse(buffer).get(), NetPacket.MULTI.NULL);
            Assert.assertEquals(5, buffer.position());
        }
        {
            buffer.clear();
            buffer.put("*0\r\n".getBytes(StandardCharsets.UTF_8)).flip();
            Assert.assertEquals(NetPacket.parse(buffer).get(), NetPacket.MULTI.BLANK);
            Assert.assertEquals(4, buffer.position());
        }
    }

    @Test
    public void read() {
        String v = "-ERROR, NOT FIND\r\n";
        ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
        for (int i = 1; i <= v.length(); i++) {
            buffer.clear();
            buffer.put(v.substring(0, i).getBytes(StandardCharsets.UTF_8));
            buffer.flip();
            Assert.assertEquals('-', buffer.get());
            Optional<? extends NetPacket> parse = NetPacket.Type.ERROR.read(buffer);
            if (i != v.length()) {
                Assert.assertFalse(parse.isPresent());
            } else {
                Assert.assertTrue(parse.isPresent());
            }
        }
    }

    @Test
    public void readToCRLF() {
        String v = "-ERROR, \rNOT\n FIND\r\nsfdsf\r\n";
        ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
        buffer.put(v.getBytes(StandardCharsets.UTF_8)).flip();
        Assert.assertEquals('-', buffer.get());
        int parse = NetPacket.gotoCRLF(buffer);
        Assert.assertEquals(20, parse);
    }

    @Test
    public void readData() {
        String v = "-ERROR, \rNOT\n FIND\r\nsfdsf\r\n" + "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
        ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
        buffer.put(v.getBytes(StandardCharsets.UTF_8)).flip();
        Assert.assertEquals('-', buffer.get());
        Optional<NetPacket.Error> to = (Optional<NetPacket.Error>) NetPacket.Type.ERROR.read(buffer);
        byte[] bytes = to.get().getData();
        Assert.assertEquals("ERROR, \rNOT\n FIND", new String(bytes));
        // System.out.println(new BigInteger(bytes).toString(16));
        // System.out.println(SafeEncoder.encode(bytes));
        Assert.assertArrayEquals(v.substring(1, 18).getBytes(StandardCharsets.UTF_8), bytes);
    }
}