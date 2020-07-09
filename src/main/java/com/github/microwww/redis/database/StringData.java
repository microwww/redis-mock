package com.github.microwww.redis.database;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.protocal.operation.StringOperation;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;

public abstract class StringData {

    //APPEND
    @NotNull
    public static ByteData append(RedisDatabase db, HashKey key, byte[] bytes) {
        return db.sync(() -> {
            ByteData v = db.getOrCreate(key, () -> {// new one
                return new ByteData(new byte[]{}, AbstractValueData.NEVER_EXPIRE);
            });
            byte[] origin = v.getData();
            byte[] target = new byte[origin.length + bytes.length];
            System.arraycopy(origin, 0, target, 0, origin.length);
            System.arraycopy(bytes, 0, target, origin.length, bytes.length);
            v.setData(target);
            return v;
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
            if (opt.equals(ByteOpt.NOT)) {// 忽略第二个参数
                res = bitOperation(res, new byte[]{}, opt);
            }
            ByteData target = new ByteData(res, AbstractValueData.NEVER_EXPIRE);
            db.put(dest, target);
            return target;
        });
    }

    public static byte[] bitOperation(byte[] data, byte[] bytes, StringData.ByteOpt opt) {
        int max = Math.max(data.length, bytes.length);
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
                return (byte) (a & b);
            }
        },
        OR {
            @Override
            public byte apply(byte a, byte b) {
                return (byte) (a | b);
            }
        },
        XOR {
            @Override
            public byte apply(byte a, byte b) {
                return (byte) (a ^ b);
            }
        },
        NOT {
            @Override
            public byte apply(byte o, byte ignore) {
                return (byte) (~o);
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
        return db.getOrCreate(key, () -> {
            return new ByteData(new byte[]{'0'}, AbstractValueData.NEVER_EXPIRE);
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

    public static final byte[] ZERO = new byte[]{};

    //GETRANGE
    public static byte[] subString(RedisDatabase db, HashKey key, int from, int includeTo) {
        Optional<ByteData> opt = db.get(key, ByteData.class);
        if (opt.isPresent()) {
            ByteData e = opt.get();
            int start = from;
            int len = e.getData().length;
            if (start < 0) {
                start += len;
            }
            int end = includeTo;
            if (end < 0) {
                end += len;
            }
            if (start > end) {
                return ZERO;
            }
            if (start >= len) {
                return ZERO;
            }
            return Arrays.copyOfRange(e.getData(), start, Math.min(len, end + 1));
        }
        return ZERO;
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
    public static boolean set(RedisDatabase db, StringOperation.SetParams spm, HashKey key, ByteData val) {
        return db.sync(() -> {
            Optional<AbstractValueData<?>> opt = db.get(key);
            if (opt.isPresent()) {
                if (spm.isXx()) { // exist to set
                    db.put(key, val);
                    return true;
                }
            } else {
                if (spm.isNx()) { // not exist to set
                    db.put(key, val);
                    return true;
                }
            }
            if (!spm.isNx() && !spm.isXx()) { // not set NX|XX
                db.put(key, val);
                return true;
            }
            return false;
        });
    }

    //SETBIT

    /**
     * @param db db
     * @param key hash-key
     * @param offset offset
     * @param one set 1/0 : true:false
     * @return true : 1, false : 0
     */
    public static boolean setBit(RedisDatabase db, HashKey key, int offset, boolean one) {
        int size = (offset >>> 3) + 1; // offset / 8
        ByteData str = db.getOrCreate(key, () -> {//
            return new ByteData(new byte[size], AbstractValueData.NEVER_EXPIRE);
        });
        byte[] od = str.getData();
        if (od.length < size) {
            byte[] ov = new byte[size];
            System.arraycopy(od, 0, ov, 0, od.length);
            str.setData(od);
        }
        BitArray st = new BitArray(str.getData());
        boolean origin = st.get(offset);
        if (one) {
            st.set(offset);
        } else {
            st.clean(offset);
        }
        str.setData(st.toArray());
        return origin;
    }
    //SETEX
    //SETNX
    //SETRANGE
    //STRLEN
}
