package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.*;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.Protocol;
import com.github.microwww.redis.util.Assert;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class StringOperation extends AbstractOperation {

    public void get(RedisRequest request) throws IOException {
        RequestParams[] args = request.getParams();
        Assert.isTrue(args.length == 1, "Must only one argument");
        HashKey key = new HashKey(args[0].getByteArray());
        RedisDatabase db = request.getDatabase();
        Optional<ByteData> val = db.get(key, ByteData.class);
        if (val.isPresent()) {
            request.getOutputProtocol().writer(val.get().getData());
        } else {
            request.getOutputProtocol().writerNull();
        }
    }

    //APPEND
    public void append(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey key = request.getParams()[0].byteArray2hashKey();
        byte[] data = request.getParams()[1].getByteArray();
        RedisDatabase db = request.getDatabase();
        ByteData append = StringData.append(db, key, data);
        request.getOutputProtocol().writer(append.getData().length);
    }

    //BITCOUNT
    public void bitcount(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        int start = 0;
        if (args.length > 1) {
            start = args[1].byteArray2int();
        }
        int end = -1;
        if (args.length > 2) {
            end = args[2].byteArray2int();
        }
        RedisDatabase db = request.getDatabase();
        int count = StringData.bitCount(db, key, start, end);
        request.getOutputProtocol().writer(count);
    }

    //BITOP
    public void bitop(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        RequestParams[] args = request.getParams();
        String opt = args[0].getByteArray2string();
        HashKey dest = args[1].byteArray2hashKey();
        HashKey key = args[2].byteArray2hashKey();
        HashKey[] hks = Arrays.stream(args, 2, args.length)
                .map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        StringData.ByteOpt bo = StringData.ByteOpt.valueOf(opt.toUpperCase());
        ByteData res = StringData.bitOperation(request.getDatabase(), bo, dest, hks);
        request.getOutputProtocol().writer(res.getData().length * 8);
    }

    //DECR
    public void decr(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        int val = StringData.increase(request.getDatabase(), key, -1);
        request.getOutputProtocol().writer(val);
    }

    //DECRBY
    public void decrby(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        int decrement = args[1].byteArray2int();
        int val = StringData.increase(request.getDatabase(), key, 0 - decrement);
        request.getOutputProtocol().writer(val);
    }

    //GET
    //GETBIT
    public void getbit(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        int offset = args[1].byteArray2int();
        int val = StringData.getBIT(request.getDatabase(), key, offset);
        request.getOutputProtocol().writer(val);
    }

    //GETRANGE
    public void getrange(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        int start = args[1].byteArray2int();
        int end = args[2].byteArray2int();
        byte[] val = StringData.subString(request.getDatabase(), key, start, end);
        request.getOutputProtocol().writer(val);
    }

    //GETSET
    public void getset(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        byte[] bytes = args[1].getByteArray();
        Optional<ByteData> val = StringData.getSet(request.getDatabase(), key, bytes);
        if (val.isPresent()) {
            request.getOutputProtocol().writer(val.get().getData());
        } else {
            request.getOutputProtocol().writerNull();
        }
    }

    //INCR
    public void incr(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        int val = StringData.increase(request.getDatabase(), key, 1);
        request.getOutputProtocol().writer(val);
    }

    //INCRBY
    public void incrby(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        int add = args[1].byteArray2int();
        int val = StringData.increase(request.getDatabase(), key, add);
        request.getOutputProtocol().writer(val);
    }

    //INCRBYFLOAT
    public void incrbyfloat(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        double add = Double.parseDouble(args[1].getByteArray2string());
        BigDecimal val = StringData.increase(request.getDatabase(), key, add);
        request.getOutputProtocol().writer(val.toPlainString());
    }

    //MGET
    public void mget(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] args = request.getParams();
        List<byte[]> list = new ArrayList<>();
        for (RequestParams arg : args) {
            HashKey hk = arg.byteArray2hashKey();
            Optional<ByteData> abs = request.getDatabase().get(hk, ByteData.class);
            list.add(abs.map(ByteData::getData).orElse(null));
        }
        byte[][] bytes = list.toArray(new byte[list.size()][]);
        request.getOutputProtocol().writerMulti(bytes);
    }

    //MSET
    public void mset(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        Assert.isTrue(args.length % 2 == 0, "key value, key value, key value");
        StringData.multiSet(request.getDatabase(), args, true);
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
    }

    //MSETNX
    public void msetnx(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        Assert.isTrue(args.length % 2 == 0, "key value, key value, key value");
        int count = StringData.multiSet(request.getDatabase(), args, false);
        request.getOutputProtocol().writer(count > 0 ? 1 : 0);
    }

    //PSETEX
    public void psetex(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        long ex = args[1].byteArray2long();
        db.put(key, new ByteData(args[2].getByteArray(), System.currentTimeMillis() + ex));
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
    }

    //SET
    public void set(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        byte[] val = args[1].getByteArray();
        Params spm = new Params();
        for (int i = 2; i < args.length; i++) {
            String op = args[i].getByteArray2string();
            Parser pm = Parser.valueOf(op.toUpperCase());
            i = pm.parse(spm, args, i);
        }
        long time = AbstractValueData.NEVER_EXPIRE;
        long now = System.currentTimeMillis();
        if (spm.milliseconds != null) {
            time = now + spm.milliseconds;
        }

        boolean set = StringData.set(db, spm, key, new ByteData(val, time));
        if (set) {
            request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
        } else {
            request.getOutputProtocol().writerNull();
        }
    }

    //SETBIT
    public void setbit(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        int off = args[1].byteArray2int();
        int val = args[2].byteArray2int();
        boolean count = StringData.setBit(db, key, off, val != 0);
        request.getOutputProtocol().writer(count ? 1 : 0);
    }

    //SETEX
    public void setex(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        long ex = args[1].byteArray2long();
        db.put(key, new ByteData(args[2].getByteArray(), System.currentTimeMillis() + ex * 1000));
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
    }

    //SETNX
    public void setnx(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        ByteData bt = db.putIfAbsent(key, new ByteData(args[1].getByteArray(), AbstractValueData.NEVER_EXPIRE));
        request.getOutputProtocol().writer(bt == null ? 1 : 0);
    }

    //SETRANGE
    public void setrange(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        HashKey key = new HashKey(args[0].getByteArray());
        int off = args[1].byteArray2int();
        byte[] val = args[2].getByteArray();
        ByteData data = StringData.setRange(request.getDatabase(), key, off, val);
        request.getOutputProtocol().writer(data.getData().length);
    }

    //STRLEN
    public void strlen(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        RequestParams[] args = request.getParams();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        int len = db.get(key, ByteData.class)
                .map(e -> e.getData().length).orElse(0);
        request.getOutputProtocol().writer(len);
    }

    public static class Params {
        private Long milliseconds;
        private boolean nx;
        private boolean xx;

        public Long getMilliseconds() {
            return milliseconds;
        }

        public void setMilliseconds(Long milliseconds) {
            this.milliseconds = milliseconds;
        }

        public boolean isNx() {
            return nx;
        }

        public void setNx(boolean nx) {
            this.nx = nx;
        }

        public boolean isXx() {
            return xx;
        }

        public void setXx(boolean xx) {
            this.xx = xx;
        }
    }

    public enum Parser {
        EX {
            @Override
            public int parse(Params params, RequestParams[] args, int i) {
                params.milliseconds = 1000L * args[i + 1].byteArray2int();
                return i + 1;
            }
        },
        PX {
            @Override
            public int parse(Params params, RequestParams[] args, int i) {
                params.milliseconds = args[i + 1].byteArray2long();
                return i + 1;
            }
        },
        NX {
            @Override
            public int parse(Params params, RequestParams[] args, int i) {
                params.nx = true;
                return i;
            }
        },
        XX {
            @Override
            public int parse(Params params, RequestParams[] args, int i) {
                params.xx = true;
                return i;
            }
        };

        public abstract int parse(Params params, RequestParams[] args, int i);
    }
}
