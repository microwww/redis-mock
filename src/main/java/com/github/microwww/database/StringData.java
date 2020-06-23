package com.github.microwww.database;

import java.util.Optional;

public abstract class StringData {

    //APPEND
    public static ByteData append(RedisDatabase db, HashKey key, byte[] bytes) {
        return db.sync(() -> {
            ByteData v = new ByteData(bytes, AbstractValueData.NEVER_EXPIRE);
            ByteData val = db.putIfAbsent(key, v);
            if (val != null) {
                byte[] d1 = v.getData();
                byte[] n = new byte[d1.length + bytes.length];
                System.arraycopy(d1, 0, n, 0, d1.length);
                System.arraycopy(bytes, 0, n, d1.length, bytes.length);
                val.setData(n);
            }
            return val;
        });
    }

    //BITCOUNT
    public static int bitCount(RedisDatabase db, HashKey key, int start, int end) {
        return db.get(key, ByteData.class).map(e -> {
            return new BitArray(e.getData()).count(true, start, end);
        }).orElse(0);
    }

    //BITOP
    public static ByteData bitOperation(RedisDatabase db, ByteOpt opt, HashKey dest, HashKey[] key) {
        ByteData init = new ByteData(new byte[]{}, -1);
        return db.sync(() -> {
            ByteData from = db.get(key[0], ByteData.class).orElse(init);
            byte[] res = from.getData();
            for (int i = 1; i < key.length; i++) {
                ByteData dta = db.get(key[i], ByteData.class).orElse(init);
                res = bitOperation(res, dta.getData(), opt);
            }
            ByteData target = new ByteData(res, AbstractValueData.NEVER_EXPIRE);
            db.put(dest, target);
            return target;
        });
    }

    public static byte[] bitOperation(byte[] data, byte[] bytes, StringData.ByteOpt opt) {
        int max = Math.max(data.length, bytes.length);
        int min = Math.min(data.length, bytes.length);
        byte[] mx = new byte[max];
        for (int i = 0; i < max; i++) {
            byte f = 0;
            if (i < data.length) {
                f = data[i];
            }
            byte t = 0;
            if (i < bytes.length) {
                t = bytes[i];
            }
            mx[i] = opt.apply(f, t);
        }
        return mx;
    }

    public enum ByteOpt {
        AND {
            @Override
            public byte apply(byte a, byte b) {
                return 0;
            }
        },
        OR {
            @Override
            public byte apply(byte a, byte b) {
                return 0;
            }
        },
        XOR {
            @Override
            public byte apply(byte a, byte b) {
                return 0;
            }
        },
        NOT {
            @Override
            public byte apply(byte a, byte b) {
                return 0;
            }
        };

        public abstract byte apply(byte a, byte b);
    }

    //DECR
    public static int decr(RedisDatabase db, HashKey key, int add) {
        return db.sync(() -> {
            Optional<ByteData> opt = db.get(key, ByteData.class);
            if (!opt.isPresent()) {
                ByteData b = new ByteData(new byte[]{0}, AbstractValueData.NEVER_EXPIRE);
                ByteData bd = db.putIfAbsent(key, b);
                opt = Optional.of(bd == null ? b : bd);
            }
            ByteData or = opt.get();
            int num = add + Integer.parseInt(new String(or.getData()));
            or.setData((num + "").getBytes());
            return num;
        });
    }

    //DECRBY
    //GET
    //GETBIT
    public static int getBIT(RedisDatabase db, HashKey key, int offset) {
        Optional<ByteData> opt = db.get(key, ByteData.class);
        return opt.map(e -> {
            BitArray b = new BitArray(e.getData());
            if (b.bitLength() > offset) {
                return b.get(offset) ? 1 : 0;
            }
            return 0;
        }).orElse(0);
    }

    //GETRANGE
    public static Optional<String> subString(RedisDatabase db, HashKey key, int from, int includeTo) {
        Optional<ByteData> opt = db.get(key, ByteData.class);
        return opt.map(e -> {
            if (from < includeTo) {
                return "";
            }
            int start = from;
            String s = new String(e.getData());
            if (start < 0) {
                start += s.length();
            }
            int end = includeTo;
            if (end < 0) {
                end += s.length();
            }
            return s.substring(Math.min(start, s.length()), Math.min(end + 1, s.length()));
        });
    }
    //GETSET
    //INCR
    //INCRBY
    //INCRBYFLOAT
    //MGET
    //MSET
    //MSETNX
    //PSETEX
    //SET
    //SETBIT
    //SETEX
    //SETNX
    //SETRANGE
    //STRLEN
}
