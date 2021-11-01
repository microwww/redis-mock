package com.github.microwww.redis.protocal;

import com.github.microwww.redis.util.Assert;

import java.nio.ByteBuffer;

public enum Prefix {
    STATUS('+') {
        @Override
        public int read(ByteBuffer bytes) {
            return Prefix.readToCRLF(bytes);
        }
    }, ERROR('-') {
        @Override
        public int read(ByteBuffer bytes) {
            return Prefix.readToCRLF(bytes);
        }
    }, INTEGER(':') {
        @Override
        public int read(ByteBuffer bytes) {
            return Prefix.readToCRLF(bytes);
        }
    }, BULK('$') {
        @Override
        public int read(ByteBuffer bytes) {
            final int from = bytes.position();
            int to = Prefix.readToCRLF(bytes);
            if (to != -1) {
                byte[] bs = Prefix.readDataSkipCRLF(bytes, from, to);
                int count = Integer.parseInt(new String(bs));
                if (bytes.remaining() >= count + 2) { // $len + \r\n + data + \r\n
                    bytes.position(to + count);
                    Assert.isTrue(CR == bytes.get(), "$ end with CR LF");
                    Assert.isTrue(LF == bytes.get(), "$ end with CR LF");
                    return bytes.position();
                }
            }
            bytes.position(from);
            return -1;
        }
    }, MULTI('*') {
        @Override
        public int read(ByteBuffer bytes) {
            final int from = bytes.position();
            int to = Prefix.readToCRLF(bytes);
            if (to != -1) {
                byte[] b1 = Prefix.readDataSkipCRLF(bytes, from, to);
                int count = Integer.parseInt(new String(b1));
                // Assert.isTrue(count >= 0, "`*` length < 0");
                if (count <= 0) {
                    return bytes.position();
                }
                for (int i = 1; ; i++) {
                    int next = parse(bytes);
                    if (next == -1) {
                        break;
                    } else if (i == count) {
                        // success
                        return bytes.position();
                    }
                }
            }
            bytes.position(from);
            return -1;
        }
    };
    public static final char CR = '\r';
    public static final char LF = '\n';
    public final char prefix;

    Prefix(char prefix) {
        this.prefix = prefix;
    }

    public static int parse(ByteBuffer bytes) {
        if (bytes.remaining() > 0) {
            int r = bytes.position();
            byte bt = bytes.get();
            for (Prefix parse : Prefix.values()) {
                if (parse.prefix == bt) {
                    int to = parse.read(bytes);
                    if (to == -1) {
                        bytes.position(r);
                        return -1;
                    }
                }
            }
            Assert.isTrue(r != bytes.position(), "Not support type: " + (char) bt);
            return bytes.position();
        }
        return -1;
    }

    public abstract int read(ByteBuffer bytes);

    /**
     * read to CRLF, return the position, and position at after CRLF. if not find CRLF will return -1 and position was not changed !
     *
     * @param bytes sources
     * @return -1 or end-position
     */
    public static int readToCRLF(ByteBuffer bytes) {
        int start = bytes.position();
        while (bytes.remaining() > 0) {
            byte bt = bytes.get();
            while (bt == CR && bytes.remaining() > 0) {
                bt = bytes.get();
                if (bt == LF) {
                    return bytes.position();
                } else if (bt == CR) {
                    continue;
                } else {
                    break;
                }
            }
        }
        bytes.position(start);
        return -1;
    }

    public static byte[] readDataSkipCRLF(ByteBuffer bytes, int from, int to) {
        Assert.isTrue(to - from >= 2, "len < 2");
        return readData(bytes, from, to - 2);
    }

    /**
     * do not modify position !
     *
     * @param bytes sources
     * @param from  form position
     * @param to    target position
     * @return from - to, byte array
     */
    public static byte[] readData(ByteBuffer bytes, int from, int to) {
        byte[] bs = new byte[to - from];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = bytes.get(from + i);
        }
        return bs;
    }
}
