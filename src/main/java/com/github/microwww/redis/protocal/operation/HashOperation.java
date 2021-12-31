package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashData;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.ScanIterator;
import com.github.microwww.redis.protocal.jedis.Protocol;
import com.github.microwww.redis.util.Assert;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class HashOperation extends AbstractOperation {

    //HDEL
    public void hdel(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(1);
        Optional<HashData> data = this.getHashData(request);
        int count = 0;
        if (data.isPresent()) {
            HashData e = data.get();
            RequestParams[] args = request.getParams();
            for (int i = 1; i < args.length; i++) {
                HashKey hk = args[i].byteArray2hashKey();
                Bytes remove = e.remove(hk);
                if (remove != null) {
                    count++;
                }
            }
        }
        request.getOutputProtocol().writer(count);
    }

    //HEXISTS
    public void hexists(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey hk = args[1].byteArray2hashKey();
        Optional<Map<HashKey, Bytes>> opt = this.getHashMap(request);
        if (opt.isPresent()) {
            boolean exist = opt.get().containsKey(hk);
            if (exist) {
                request.getOutputProtocol().writer(1);
                return;
            }
        }
        request.getOutputProtocol().writer(0);
    }

    //HGET
    public void hget(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey hk = args[1].byteArray2hashKey();
        Optional<HashData> opt = this.getHashData(request);
        if (opt.isPresent()) {
            Bytes dh = opt.get().getData().get(hk);
            if (dh != null) {
                request.getOutputProtocol().writer(dh);
                return;
            }
        }
        request.getOutputProtocol().writerNull();
    }

    //HGETALL
    public void hgetall(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<Map<HashKey, Bytes>> opt = this.getHashMap(request);
        if (opt.isPresent()) {
            // Map<HashKey, byte[]> map = new HashMap<>(opt.get());
            List<byte[]> list = new ArrayList<>();
            opt.get().forEach((k, v) -> {
                list.add(k.getBytes());
                list.add(v.getBytes());
            });
            request.getOutputProtocol().writerMulti(list.toArray(new byte[list.size()][]));
        } else {
            request.getOutputProtocol().writerMulti(); //  null ?
        }
    }

    //HINCRBY
    public void hincrby(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        HashKey key = request.getParams()[0].byteArray2hashKey();
        HashKey hk = request.getParams()[1].byteArray2hashKey();
        int inc = request.getParams()[2].byteArray2int();
        HashData oc = this.getOrCreate(request);
        Bytes bytes = oc.incrBy(hk, inc);
        request.getOutputProtocol().writer(bytes.toLong());
    }

    //HINCRBYFLOAT
    public void hincrbyfloat(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        HashKey key = request.getParams()[0].byteArray2hashKey();
        HashKey hk = request.getParams()[1].byteArray2hashKey();
        BigDecimal inc = request.getParams()[2].byteArray2decimal();
        HashData oc = this.getOrCreate(request);
        Bytes bytes = oc.incrByFloat(hk, inc);
        request.getOutputProtocol().writer(bytes);
    }

    //HKEYS
    public void hkeys(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<Map<HashKey, Bytes>> map = this.getHashMap(request);
        if (map.isPresent()) {
            byte[][] ks = map.get().keySet().stream().map(HashKey::getBytes).toArray(byte[][]::new);
            request.getOutputProtocol().writerMulti(ks);
        } else {
            request.getOutputProtocol().writerMulti();
        }
    }

    //HLEN
    public void hlen(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<Map<HashKey, Bytes>> map = this.getHashMap(request);
        int count = map.map(e -> e.size()).orElse(0);
        request.getOutputProtocol().writer(count);
    }

    //HMGET
    public void hmget(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        Optional<Map<HashKey, Bytes>> opt = this.getHashMap(request);
        byte[][] bytes = Arrays.stream(args, 1, args.length)
                .map(e -> e.byteArray2hashKey())
                .map(e -> {//
                    return opt.flatMap(e1 -> Optional.ofNullable(e1.get(e))).map(Bytes::getBytes).orElse(null);
                }).toArray(byte[][]::new);
        request.getOutputProtocol().writerMulti(bytes); //  null ?
    }

    //HMSET
    public void hmset(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(3);
        HashData oc = this.getOrCreate(request);
        RequestParams[] args = request.getParams();
        Assert.isTrue(args.length % 2 == 1, "k, k-v, k-v, k-v, ...");
        oc.multiSet(args, 1);
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name()); //  null ?
    }

    //HSET
    public void hset(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();

        HashData data = this.getOrCreate(request);
        byte[] hk = args[1].getByteArray();
        byte[] val = args[2].getByteArray();

        Bytes origin = data.put(new HashKey(hk), val);

        // new: 1, over-write: 0
        request.getOutputProtocol().writer(origin == null ? 1 : 0);
    }

    //HSETNX
    public void hsetnx(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();

        HashData data = this.getOrCreate(request);
        byte[] hk = args[1].getByteArray();
        byte[] val = args[2].getByteArray();

        Bytes origin = data.putIfAbsent(new HashKey(hk), val);

        request.getOutputProtocol().writer(origin == null ? 1 : 0);
    }

    //HVALS
    public void hvals(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<Map<HashKey, Bytes>> map = this.getHashMap(request);
        if (map.isPresent()) {
            byte[][] ks = map.get().values().stream().map(Bytes::getBytes).toArray(byte[][]::new);
            request.getOutputProtocol().writerMulti(ks);
        } else {
            request.getOutputProtocol().writerMulti();
        }
    }

    //HSCAN
    public void hscan(RedisRequest request) throws IOException {
        HashData data = this.getHashData(request).orElse(new HashData());
        Set<HashKey> hk = data.getData().keySet();
        Iterator<HashKey> iterator = hk.iterator();
        new ScanIterator<HashKey>(request, 1)
                .skip(iterator)
                .continueWrite(iterator, e -> {// key
                    return e.getBytes();
                }, e -> {// value
                    return data.getData().get(e).getBytes();
                });
    }

    private Optional<Map<HashKey, Bytes>> getHashMap(RedisRequest request) {
        return getHashData(request).map(e -> e.getData());
    }

    private Optional<HashData> getHashData(RedisRequest request) {
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        return request.getDatabase().get(key, HashData.class);
    }

    private HashData getOrCreate(RedisRequest request) {
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        return request.getDatabase().getOrCreate(key, () -> {//
            return new HashData();
        });
    }
}
