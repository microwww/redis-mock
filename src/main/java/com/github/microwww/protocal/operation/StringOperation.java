package com.github.microwww.protocal.operation;

import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.database.*;
import com.github.microwww.protocal.AbstractOperation;
import com.github.microwww.protocal.RedisOutputProtocol;
import com.github.microwww.protocal.RedisRequest;
import com.github.microwww.util.Assert;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class StringOperation extends AbstractOperation {

    public void get(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 1, "Must only one argument");
        HashKey key = new HashKey(args[0].getByteArray());
        RedisDatabase db = request.getDatabase();
        Optional<ByteData> val = db.get(key, ByteData.class);
        if (val.isPresent()) {
            RedisOutputProtocol.writer(request.getOutputStream(), val.get().getData());
        } else {
            RedisOutputProtocol.writerNull(request.getOutputStream());
        }
    }

    //APPEND
    public void append(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        byte[] data = request.getArgs()[1].getByteArray();
        RedisDatabase db = request.getDatabase();
        ByteData append = StringData.append(db, key, data);
        RedisOutputProtocol.writer(request.getOutputStream(), append.getData().length);
    }

    //BITCOUNT
    public void bitcount(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
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
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //BITOP
    public void bitop(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        ExpectRedisRequest[] args = request.getArgs();
        String opt = args[0].getByteArray2string();
        HashKey dest = args[1].byteArray2hashKey();
        HashKey key = args[2].byteArray2hashKey();
        HashKey[] hks = Arrays.stream(args, 2, args.length)
                .map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        StringData.ByteOpt bo = StringData.ByteOpt.valueOf(opt.toUpperCase());
        ByteData res = StringData.bitOperation(request.getDatabase(), bo, dest, hks);
        RedisOutputProtocol.writer(request.getOutputStream(), res.getData().length * 8);
    }

    //DECR
    public void decr(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = args[0].byteArray2hashKey();
        int val = StringData.increase(request.getDatabase(), key, -1);
        RedisOutputProtocol.writer(request.getOutputStream(), val);
    }

    //DECRBY
    public void decrby(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = args[0].byteArray2hashKey();
        int decrement = args[1].byteArray2int();
        int val = StringData.increase(request.getDatabase(), key, 0 - decrement);
        RedisOutputProtocol.writer(request.getOutputStream(), val);
    }

    //GET
    //GETBIT
    public void getbit(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = args[0].byteArray2hashKey();
        int offset = args[1].byteArray2int();
        int val = StringData.getBIT(request.getDatabase(), key, offset);
        RedisOutputProtocol.writer(request.getOutputStream(), val);
    }

    //GETRANGE
    public void getrange(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = args[0].byteArray2hashKey();
        int start = args[1].byteArray2int();
        int end = args[2].byteArray2int();
        Optional<String> val = StringData.subString(request.getDatabase(), key, start, end);
        RedisOutputProtocol.writer(request.getOutputStream(), val.orElse("").getBytes());
    }

    //GETSET
    public void getset(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = args[0].byteArray2hashKey();
        byte[] bytes = args[1].getByteArray();
        Optional<ByteData> val = StringData.getSet(request.getDatabase(), key, bytes);
        if (val.isPresent()) {
            RedisOutputProtocol.writer(request.getOutputStream(), val.get().getData());
        } else {
            RedisOutputProtocol.writerNull(request.getOutputStream());
        }
    }

    //INCR
    public void incr(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = args[0].byteArray2hashKey();
        int val = StringData.increase(request.getDatabase(), key, 1);
        RedisOutputProtocol.writer(request.getOutputStream(), val);
    }

    //INCRBY
    public void incrby(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = args[0].byteArray2hashKey();
        int add = args[1].byteArray2int();
        int val = StringData.increase(request.getDatabase(), key, add);
        RedisOutputProtocol.writer(request.getOutputStream(), val);
    }

    //INCRBYFLOAT
    public void incrbyfloat(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = args[0].byteArray2hashKey();
        double add = Double.parseDouble(args[1].getByteArray2string());
        BigDecimal val = StringData.increase(request.getDatabase(), key, add);
        RedisOutputProtocol.writer(request.getOutputStream(), val.toPlainString());
    }

    //MGET
    public void mget(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        List<byte[]> list = new ArrayList<>();
        for (ExpectRedisRequest arg : args) {
            HashKey hk = arg.byteArray2hashKey();
            Optional<ByteData> abs = request.getDatabase().get(hk, ByteData.class);
            list.add(abs.map(ByteData::getData).orElse(null));
        }
        byte[][] bytes = list.toArray(new byte[list.size()][]);
        RedisOutputProtocol.writerMulti(request.getOutputStream(), bytes);
    }

    //MSET
    public void mset(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length % 2 == 0, "key value, key value, key value");
        StringData.multiSet(request.getDatabase(), args, true);
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    //MSETNX
    public void msetnx(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length % 2 == 0, "key value, key value, key value");
        int count = StringData.multiSet(request.getDatabase(), args, false);
        RedisOutputProtocol.writer(request.getOutputStream(), count > 0 ? 1 : 0);
    }

    //PSETEX
    public void psetex(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        long ex = args[2].byteArray2long();
        db.put(key, new ByteData(args[1].getByteArray(), System.currentTimeMillis() + ex));
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    //SET
    public void set(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        db.put(key, args[1].getByteArray());
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    //SETBIT
    //SETEX
    public void setex(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        long ex = args[2].byteArray2long();
        db.put(key, new ByteData(args[1].getByteArray(), System.currentTimeMillis() + ex * 1000));
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    //SETNX
    public void setnx(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        db.putIfAbsent(key, new ByteData(args[1].getByteArray(), AbstractValueData.NEVER_EXPIRE));
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    //SETRANGE
    public void setrange(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = new HashKey(args[0].getByteArray());
        int off = args[1].byteArray2int();
        byte[] val = args[2].getByteArray();
        ByteData data = StringData.setRange(request.getDatabase(), key, off, val);
        RedisOutputProtocol.writer(request.getOutputStream(), data.getData().length);
    }

    //STRLEN
    public void strlen(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        ExpectRedisRequest[] args = request.getArgs();
        RedisDatabase db = request.getDatabase();
        HashKey key = new HashKey(args[0].getByteArray());
        int len = db.get(key, ByteData.class)
                .map(e -> e.getData().length).orElse(0);
        RedisOutputProtocol.writer(request.getOutputStream(), len);
    }
}
