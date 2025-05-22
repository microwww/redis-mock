package com.github.microwww.redis.protocal.message;

import com.github.microwww.redis.protocal.HalfPackException;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;
import com.github.microwww.redis.util.Nullable;
import com.github.microwww.redis.util.SafeEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public enum Type {

    STATUS('+') {
        @Override
        public StringMessage read(ByteBuffer bytes) {
            return new StringMessage(this, Type.readToCRLF(bytes));
        }
    }, ERROR('-') {
        @Override
        public ErrorMessage read(ByteBuffer bytes) {
            return new ErrorMessage(this, Type.readToCRLF(bytes));
        }
    }, ERRORS('!') {
        @Override
        public ErrorMessage read(ByteBuffer bytes) {
            return new ErrorMessage(this, BULK.read(bytes).getBytes());
        }
    }, LONG(':') {
        @Override
        public LongMessage read(ByteBuffer bytes) {
            return new LongMessage(this, Type.readToCRLF(bytes));
        }
    }, EOF('.') {
        @Override
        public EOFMessage read(ByteBuffer bytes) {
            Type.gotoCRLF(bytes);
            return new EOFMessage(this);
        }
    }, GO_ON(';') {
        @Override
        public RedisMessage read(ByteBuffer bytes) {
            int len = Integer.parseInt(SafeEncoder.encode(readInt(bytes)));
            if (len <= 0) return new EOFMessage(this);
            byte[] bts = readDataSkipCRLF(bytes, len);
            return new BytesMessage(this, bts);
        }
    }, BULK('$') {
        @Override
        public StringMessage read(ByteBuffer bytes) {
            String s = new String(Type.readToCRLF(bytes));
            if ("?".equalsIgnoreCase(s)) {
                // $?<CR><LF>;4<CR><LF>Hell<CR><LF>;5<CR><LF>o wor<CR><LF>;1<CR><LF>d<CR><LF>;0<CR><LF>
                StringBuilder sb = new StringBuilder();
                while (true) {
                    assertNotHalf(bytes, "$? GO-ON");
                    RedisMessage r = Type.parseOne(bytes);
                    if (r instanceof EOFMessage) {
                        break;
                    }
                    Assert.isTrue(r instanceof BytesMessage, ";<NUMBER><CR><LF>");
                    sb.append(SafeEncoder.encode(r.getBytes()));
                }
                return new StringMessage(this, SafeEncoder.encode(sb.toString()));
            }
            int count = Integer.parseInt(s);
            if (count >= 0) {
                byte[] data = readDataSkipCRLF(bytes, count);
                // $len + \r\n + data + \r\n
                return new StringMessage(this, data);
            }
            return new StringMessage(this, null);
        }
    }, MULTI('*') {
        @Override
        public MultiMessage read(ByteBuffer bytes) {
            String s = new String(Type.readToCRLF(bytes));
            if ("?".equalsIgnoreCase(s)) {
                // *?<CR><LF>:4<CR><LF>Hell<CR><LF>:5<CR><LF>o wor<CR><LF>:1<CR><LF>d<CR><LF>.<CR><LF>
                List<RedisMessage> list = new ArrayList<>();
                while (true) {
                    assertNotHalf(bytes, "*? GO-ON");
                    RedisMessage r = Type.parseOne(bytes);
                    if (r instanceof EOFMessage) {
                        break;
                    }
                    list.add(r);
                }
                return new MultiMessage(this, list.toArray(new RedisMessage[0]));
            }

            int count = Integer.parseInt(s);
            if (count >= 0) {
                // Assert.isTrue(count >= 0, "`*` length < 0");
                if (count == 0) {
                    Type.readToCRLF(bytes);
                    return new MultiMessage(this, new RedisMessage[]{});
                }
                RedisMessage[] pks = new RedisMessage[count];
                for (int i = 0; ; i++) {
                    pks[i] = parseOne(bytes);
                    if (i + 1 == count) {// success
                        return new MultiMessage(this, pks);
                    }
                }
            }
            return null;
        }
    }, NULL('_') {
        @Override
        public NullMessage read(ByteBuffer bytes) {
            byte[] n = Type.readToCRLF(bytes);
            Assert.isTrue(n.length == 0, "`_<CR><LF>`");
            return new NullMessage(this);
        }
    }, DECIMAL(',') {
        @Override
        public DoubleMessage read(ByteBuffer bytes) {
            return new DoubleMessage(this, Type.readToCRLF(bytes));
        }
    }, BOOLEAN('#') {
        @Override
        public BooleanMessage read(ByteBuffer bytes) {
            byte[] bts = Type.readToCRLF(bytes);
            return new BooleanMessage(this, bts);
        }
    }, VERBATIM('=') {
        @Override
        public VerbatimMessage read(ByteBuffer bytes) {
            byte[] len = Type.readToCRLF(bytes);
            byte[] bts = Type.readDataSkipCRLF(bytes, Integer.parseInt(new String(len)));
            return new VerbatimMessage(this, bts);
        }
    }, BigInt('(') {
        @Override
        public BigIntMessage read(ByteBuffer bytes) {
            return new BigIntMessage(this, Type.readToCRLF(bytes));
        }
    }, MAP('%') {
        @Override
        public MapMessage read(ByteBuffer bytes) {
            byte[] rf = Type.readToCRLF(bytes);
            int count = Integer.parseInt(new String(rf));
            RedisMessage[] ns = new RedisMessage[count * 2];
            for (int i = 0; i < ns.length; i++) {
                ns[i] = parseOne(bytes);
            }
            return new MapMessage(this, ns);
        }
    }, SETS('~') {
        @Override
        public SetsMessage read(ByteBuffer bytes) {
            RedisMessage read = MULTI.read(bytes);
            return new SetsMessage(this, read.getRedisMessages());
        }
    }, ATTR('|') {
        @Override
        public RedisMessage read(ByteBuffer bytes) {
            RedisMessage m = MAP.read(bytes);
            AttrMessage arr = new AttrMessage(this, m.getRedisMessages());
            assertNotHalf(bytes, "Attr must has next data");
            RedisMessage one = Type.parseOne(bytes);
            return one.setAttr(arr);
        }
    }, PUSH('>') {
        @Override
        public PushMessage read(ByteBuffer bytes) {
            RedisMessage rm = MULTI.read(bytes);
            return new PushMessage(this, rm.getRedisMessages());
        }
    },
    ;
    public final char prefix;

    @Nullable
    public abstract RedisMessage read(@NotNull ByteBuffer bytes);

    Type(char prefix) {
        this.prefix = prefix;
    }

    public static final char CR = '\r';
    public static final char LF = '\n';

    public static void assertNotHalf(ByteBuffer bytes, String message) {
        if (bytes.remaining() <= 0) {
            throw new HalfPackException(message);
        }
    }

    public static RedisMessage parseOne(ByteBuffer bytes) throws HalfPackException {
        assertNotHalf(bytes, "No data to parse");
        byte bt = bytes.get();
        for (Type parse : Type.values()) {
            if (parse.prefix == bt) {
                return parse.read(bytes);
            }
        }
        throw new IllegalArgumentException("Not support type: " + (char) bt);
    }

    /**
     * read to CRLF, return the position, and position at after CRLF. if not find CRLF will return -1 and position was not changed !
     *
     * @param bytes sources
     * @return -1 or end-position
     */
    public static int gotoCRLF(ByteBuffer bytes) throws HalfPackException {
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
        throw new HalfPackException();
    }

    @Nullable
    public static byte[] readToCRLF(ByteBuffer bytes) {
        int start = bytes.position();
        int to = Type.gotoCRLF(bytes);
        if (to < 0) {
            return null;
        }
        return Type.getData(bytes, start, to - 2);
    }

    @Nullable
    public static byte[] readInt(ByteBuffer bytes) {
        byte[] bts = readToCRLF(bytes);
        if (bts == null) return new byte[]{};
        return bts;
    }

    public static byte[] readDataSkipCRLF(ByteBuffer bytes, int length) {
        if (bytes.remaining() < length + 2) throw new HalfPackException("Read length: " + length);

        int pt = bytes.position();
        byte[] data = getData(bytes, pt, pt + length);
        bytes.position(pt + length);

        Assert.isTrue(CR == bytes.get(), "end with CR LF");
        Assert.isTrue(LF == bytes.get(), "end with CR LF");

        return data;
    }

    /**
     * do not modify position !
     *
     * @param bytes sources
     * @param from  form position
     * @param to    target position
     * @return from - to, byte array
     */
    public static byte[] getData(ByteBuffer bytes, int from, int to) {
        if (bytes.limit() < to) {
            throw new HalfPackException();
        }
        byte[] bs = new byte[to - from];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = bytes.get(from + i);
        }
        return bs;
    }
}