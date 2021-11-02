package com.github.microwww.redis.protocal;

import com.github.microwww.redis.util.Assert;

import java.nio.ByteBuffer;
import java.util.Optional;

public abstract class NetPacket<T> {
    private final T data;
    public final Type type;

    public NetPacket(T data, Type type) {
        this.data = data;
        this.type = type;
    }

    public T getData() {
        return data;
    }

    public static class Status extends NetPacket<byte[]> {
        public Status(byte[] bytes) {
            super(bytes, Type.STATUS);
        }
    }

    public static class Error extends NetPacket<byte[]> {
        public Error(byte[] data) {
            super(data, Type.ERROR);
        }
    }

    public static class BigInt extends NetPacket<Long> {
        public BigInt(Long data) {
            super(data, Type.BigInt);
            Assert.isTrue(data != null, "Not null");
        }

        public int toInt() {
            return this.getData().intValue();
        }
    }

    public static class BULK extends NetPacket<byte[]> {
        public BULK(byte[] data) {
            super(data, Type.BULK);
            Assert.isTrue(data != null, "Not null");
        }
    }

    public static class MULTI<T extends NetPacket> extends NetPacket<T[]> {
        public static final MULTI NULL = new MULTI(null);
        public static final MULTI BLANK = new MULTI(new NetPacket[]{});

        public MULTI(T[] data) {
            super(data, Type.MULTI);
        }
    }

    public enum Type {

        STATUS('+') {
            @Override
            public Optional<Status> read(ByteBuffer bytes) {
                return NetPacket.readToCRLF(bytes).map(Status::new);
            }
        }, ERROR('-') {
            @Override
            public Optional<Error> read(ByteBuffer bytes) {
                return NetPacket.readToCRLF(bytes).map(Error::new);
            }
        }, BigInt(':') {
            @Override
            public Optional<BigInt> read(ByteBuffer bytes) {
                return NetPacket.readToCRLF(bytes).map(String::new).map(Long::valueOf).map(BigInt::new);
            }
        }, BULK('$') {
            @Override
            public Optional<BULK> read(ByteBuffer bytes) {
                final int from = bytes.position();
                Optional<BigInt> to = (Optional<NetPacket.BigInt>) BigInt.read(bytes);
                if (to.isPresent()) {
                    int count = to.get().toInt();
                    int position = bytes.position();
                    if (bytes.remaining() >= count + 2) { // $len + \r\n + data + \r\n
                        bytes.position(position + count);
                        Assert.isTrue(CR == bytes.get(), "$ end with CR LF");
                        Assert.isTrue(LF == bytes.get(), "$ end with CR LF");
                        byte[] bts = NetPacket.getDataSkipCRLF(bytes, position, bytes.position());
                        return Optional.of(new BULK(bts));
                    }
                }
                bytes.position(from);
                return Optional.empty();
            }
        }, MULTI('*') {
            @Override
            public Optional<MULTI> read(ByteBuffer bytes) {
                final int from = bytes.position();
                Optional<BigInt> to = (Optional<NetPacket.BigInt>) BigInt.read(bytes);
                if (to.isPresent()) {
                    int count = to.get().toInt();
                    // Assert.isTrue(count >= 0, "`*` length < 0");
                    if (count == -1) {
                        return Optional.of(NetPacket.MULTI.NULL);
                    } else if (count == 0) {
                        return Optional.of(NetPacket.MULTI.BLANK);
                    }
                    NetPacket[] pks = new NetPacket[count];
                    for (int i = 0; ; i++) {
                        Optional<? extends NetPacket> next = parse(bytes);
                        if (!next.isPresent()) {
                            break;
                        } else {
                            pks[i] = next.get();
                        }
                        if (i + 1 == count) {
                            // success
                            return Optional.of(new MULTI(pks));
                        }
                    }
                }
                bytes.position(from);
                return Optional.empty();
            }
        };
        public final char prefix;

        public abstract Optional<? extends NetPacket> read(ByteBuffer bytes);

        Type(char prefix) {
            this.prefix = prefix;
        }
    }

    public static final char CR = '\r';
    public static final char LF = '\n';

    public static Optional<? extends NetPacket> parse(ByteBuffer bytes) {
        if (bytes.remaining() > 0) {
            int r = bytes.position();
            byte bt = bytes.get();
            for (Type parse : Type.values()) {
                if (parse.prefix == bt) {
                    Optional<? extends NetPacket> to = parse.read(bytes);
                    if (to.isPresent()) {
                        return to;
                    } else {
                        bytes.position(r);
                        return Optional.empty();
                    }
                }
            }
            throw new IllegalArgumentException("Not support type: " + (char) bt);
        }
        return Optional.empty();
    }

    /**
     * read to CRLF, return the position, and position at after CRLF. if not find CRLF will return -1 and position was not changed !
     *
     * @param bytes sources
     * @return -1 or end-position
     */
    public static int gotoCRLF(ByteBuffer bytes) {
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

    public static Optional<byte[]> readToCRLF(ByteBuffer bytes) {
        int start = bytes.position();
        int to = NetPacket.gotoCRLF(bytes);
        if (to < 0) {
            return Optional.empty();
        }
        return Optional.of(NetPacket.getDataSkipCRLF(bytes, start, to));
    }

    public static byte[] getDataSkipCRLF(ByteBuffer bytes, int from, int to) {
        Assert.isTrue(to - from >= 2, "len < 2");
        return getData(bytes, from, to - 2);
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
        byte[] bs = new byte[to - from];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = bytes.get(from + i);
        }
        return bs;
    }
}
