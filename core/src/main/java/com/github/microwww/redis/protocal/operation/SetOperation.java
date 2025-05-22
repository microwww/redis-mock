package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.database.SetData;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.ScanIterator;

import java.io.IOException;
import java.util.*;

public class SetOperation extends AbstractOperation {

    //SADD
    public void sadd(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        HashKey hk = args[0].byteArray2hashKey();
        SetData ss = request.getDatabase().getOrCreate(hk, SetData::new);
        Bytes[] ms = Arrays.stream(args, 1, args.length).map(e -> e.toBytes())
                .toArray(Bytes[]::new);

        int count = ss.add(ms);
        request.getOutputProtocol().writer(count);
    }

    //SCARD
    public void scard(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        RequestParams[] args = request.getParams();
        HashKey hk = args[0].byteArray2hashKey();
        Optional<SetData> ss = request.getDatabase().get(hk, SetData.class);

        int size = ss.map(e -> e.getData().size()).orElse(0);
        request.getOutputProtocol().writer(size);
    }

    //SDIFF
    public void sdiff(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] args = request.getParams();
        HashKey source = args[0].byteArray2hashKey();
        Optional<SetData> first = request.getDatabase().get(source, SetData.class);

        HashKey[] keys = Arrays.stream(args, 1, args.length).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);

        byte[][] res = new byte[0][];
        if (first.isPresent()) {
            res = first.get().diff(request.getDatabase(), keys) //
                    .stream().map(Bytes::getBytes).toArray(byte[][]::new);
        }
        request.getOutputProtocol().writerMulti(res);
    }

    //SDIFFSTORE
    public void sdiffstore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        HashKey[] keys = Arrays.stream(args, 2, args.length).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);

        Optional<SetData> oc = request.getDatabase().get(args[1].byteArray2hashKey(), SetData.class);
        Set<Bytes> res = Collections.emptySet();
        if (oc.isPresent()) {
            SetData data = oc.get().diffStore(request.getDatabase(), args[0].byteArray2hashKey(), keys);
            res = data.getData();
        }

        request.getOutputProtocol().writer(res.size());
    }

    //SINTER
    public void sinter(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] args = request.getParams();

        HashKey[] keys = Arrays.stream(args).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        Set<Bytes> bytes = new SetData().interStore(request.getDatabase(), keys);
        byte[][] res = bytes.stream().map(Bytes::getBytes).toArray(byte[][]::new);

        request.getOutputProtocol().writerMulti(res);
    }

    //SINTERSTORE
    public void sinterstore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] args = request.getParams();

        HashKey[] keys = Arrays.stream(args, 1, args.length).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        RedisDatabase db = request.getDatabase();
        SetData dat = db.sync(() -> {
            SetData oc = new SetData();
            oc.interStore(db, keys);
            db.put(args[0].byteArray2hashKey(), oc);
            return oc;
        });

        request.getOutputProtocol().writer(dat.getData().size());
    }

    //SISMEMBER
    public void sismember(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
        HashKey hk = args[0].byteArray2hashKey();

        Optional<SetData> data = request.getDatabase().get(hk, SetData.class);
        Bytes bts = new Bytes(args[1].getByteArray());
        boolean exist = data.map(e -> e.getData().contains(bts)).orElse(false);

        request.getOutputProtocol().writer(exist ? 1 : 0);
    }

    //SMEMBERS
    public void smembers(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        RequestParams[] args = request.getParams();
        HashKey hk = args[0].byteArray2hashKey();

        Optional<SetData> data = request.getDatabase().get(hk, SetData.class);
        byte[][] bytes = new byte[0][];
        if (data.isPresent()) {
            bytes = data.get().getData()
                    .stream()
                    .map(Bytes::getBytes)
                    .toArray(byte[][]::new);
        }

        request.getOutputProtocol().writerMulti(bytes);
    }

    //SMOVE
    public void smove(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
        HashKey source = args[0].byteArray2hashKey();
        HashKey dest = args[1].byteArray2hashKey();
        Bytes member = args[2].toBytes();

        Optional<SetData> data = request.getDatabase().get(source, SetData.class);
        boolean move = false;
        if (data.isPresent()) {
            move = data.get().move(request.getDatabase(), dest, member);
        }

        request.getOutputProtocol().writer(move ? 1 : 0);
    }

    //SPOP
    public void spop(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] args = request.getParams();
        HashKey source = args[0].byteArray2hashKey();
        Optional<SetData> data = request.getDatabase().get(source, SetData.class);
        if (args.length > 1) {
            int count = args[1].byteArray2int();
            byte[][] bytes = data.map(e -> e.pop(count))
                    .orElse(Collections.emptyList())
                    .stream().map(Bytes::getBytes)
                    .toArray(byte[][]::new);
            request.getOutputProtocol().writerMulti(bytes);
        } else {
            Optional<Bytes> bytes = data.map(e -> e.pop(1)).flatMap(e -> {
                if (e.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(e.get(0));
            });
            request.getOutputProtocol().writer(bytes.map(Bytes::getBytes).orElse(null));
        }
    }

    //SRANDMEMBER
    public void srandmember(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] args = request.getParams();
        HashKey source = args[0].byteArray2hashKey();
        Optional<SetData> data = request.getDatabase().get(source, SetData.class);

        if (args.length > 1) {
            List<Bytes> list = Collections.emptyList();
            int count = args[1].byteArray2int();
            if (count > 0) {
                list = data.map(e -> e.randMember(count)).orElse(Collections.emptyList());
            } else if (count < 0) {
                list = data.map(e -> e.random(0 - count)).orElse(Collections.emptyList());
            }
            byte[][] bytes = list.stream().map(Bytes::getBytes).toArray(byte[][]::new);
            request.getOutputProtocol().writerMulti(bytes);
        } else {
            Optional<Bytes> bytes = data.map(e -> e.randMember(1)).flatMap(e -> {
                if (e.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(e.get(0));
            });
            request.getOutputProtocol().writer(bytes.map(Bytes::getBytes).orElse(null));
        }

    }

    //SREM
    public void srem(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);

        RequestParams[] args = request.getParams();
        HashKey source = args[0].byteArray2hashKey();
        Optional<SetData> data = request.getDatabase().get(source, SetData.class);

        int count = 0;
        if (data.isPresent()) {
            Bytes[] bts = Arrays.stream(args, 1, args.length).map(e -> e.toBytes())
                    .toArray(Bytes[]::new);
            count = data.get().removeAll(bts);
        }

        request.getOutputProtocol().writer(count);
    }

    //SUNION
    public void sunion(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] args = request.getParams();
        HashKey[] keys = Arrays.stream(args).map(e -> e.byteArray2hashKey())
                .toArray(HashKey[]::new);
        Set<Bytes> union = new SetData().union(request.getDatabase(), keys);

        request.getOutputProtocol().writerMulti(union.stream().map(Bytes::getBytes).toArray(byte[][]::new));
    }

    //SUNIONSTORE
    public void sunionstore(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        RedisDatabase db = request.getDatabase();
        SetData union = db.sync(() -> {
            HashKey[] keys = Arrays.stream(args, 1, args.length).map(e -> e.byteArray2hashKey())
                    .toArray(HashKey[]::new);
            SetData dest = new SetData();
            dest.union(db, keys);
            db.put(args[0].byteArray2hashKey(), dest);
            return dest;
        });

        request.getOutputProtocol().writer(union.getData().size());
    }

    //SSCAN
    public void sscan(RedisRequest request) throws IOException {
        RequestParams[] args = request.getParams();
        HashKey key = args[0].byteArray2hashKey();
        Optional<SetData> opt = request.getDatabase().get(key, SetData.class);
        Set<Bytes> hk = opt.map(e -> e.getData()).orElse(Collections.emptySet());
        Iterator<Bytes> iterator = hk.iterator();
        new ScanIterator<Bytes>(request, 1)
                .skip(iterator)
                .continueWrite(iterator, e -> {// key
                    return e.getBytes();
                });
    }

    public SetData getOrCreate(RedisRequest request) {
        RequestParams[] args = request.getParams();
        HashKey source = args[0].byteArray2hashKey();
        return request.getDatabase().getOrCreate(source, SetData::new);
    }
}
