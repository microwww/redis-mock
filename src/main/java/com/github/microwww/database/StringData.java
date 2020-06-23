package com.github.microwww.database;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.util.Assert;

import java.math.BigDecimal;
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

    public static Optional<ByteData> getSet(RedisDatabase db, HashKey key, byte[] value) {
        return db.sync(() -> {
            Optional<ByteData> val = db.get(key, ByteData.class); // check type !!!
            db.put(key, new ByteData(value, AbstractValueData.NEVER_EXPIRE));
            return val;
        });
    }

    public static int multiSet(RedisDatabase database, ExpectRedisRequest[] args, boolean overWrite) {
        Assert.isTrue(args.length % 2 == 0, "2x");
        return database.sync(() -> {
            if (!overWrite) {
                for (int i = 0; i < args.length; i += 2) {
                    HashKey key = args[i].byteArray2hashKey();
                    Optional<AbstractValueData<?>> opt = database.get(key);
                    if (opt.isPresent()) {
                        return 0;
                    }
                }
            }
            for (int i = 0; i < args.length; i += 2) {
                HashKey key = args[i].byteArray2hashKey();
                byte[] val = args[i + 1].getByteArray();
                database.put(key, val);
            }
            return args.length / 2;
        });
    }

    public static ByteData setRange(RedisDatabase database, HashKey key, int off, byte[] val) {
        Assert.isTrue(off >= 0, "offset >= 0");
        Assert.isTrue(Integer.MAX_VALUE - off > val.length, "Over max int !");
        return database.sync(() -> {
            Optional<ByteData> opt = database.get(key, ByteData.class);
            if (opt.isPresent()) {
                byte[] org = opt.get().getData();
                if (org.length < off + val.length) {
                    byte[] bts = new byte[off + val.length];
                    System.arraycopy(org, 0, bts, 0, org.length);
                    opt.get().setData(bts);
                }
            } else {
                opt = Optional.of(new ByteData(new byte[off + val.length], AbstractValueData.NEVER_EXPIRE));
                database.put(key, opt.get());
            }
            System.arraycopy(val, 0, opt.get().getData(), off, val.length);
            return opt.get();
        });
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
    public static int increase(RedisDatabase db, HashKey key, int add) {
        return db.sync(() -> {
            ByteData or = getOrInitZero(db, key);
            int num = add + Integer.parseInt(new String(or.getData()));
            or.setData((num + "").getBytes());
            return num;
        });
    }

    public static BigDecimal increase(RedisDatabase db, HashKey key, double add) {
        return db.sync(() -> {
            ByteData or = getOrInitZero(db, key);
            BigDecimal num = new BigDecimal(new String(or.getData())).add(BigDecimal.valueOf(add));
            or.setData(num.toPlainString().getBytes());
            return num;
        });
    }

    private static ByteData getOrInitZero(RedisDatabase db, HashKey key) {
        Optional<ByteData> opt = db.get(key, ByteData.class);
        if (!opt.isPresent()) {
            ByteData b = new ByteData(new byte[]{0}, AbstractValueData.NEVER_EXPIRE);
            ByteData bd = db.putIfAbsent(key, b);
            opt = Optional.of(bd == null ? b : bd);
        }
        return opt.get();
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
