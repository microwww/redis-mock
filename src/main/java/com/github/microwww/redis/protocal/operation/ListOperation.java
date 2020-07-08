package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ConsumerIO;
import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.ListData;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.exception.RequestInterruptedException;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.util.NotNull;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ListOperation extends AbstractOperation {
    private static final long MAX_WAIT_SECONDS = 60 * 60 * 24 * 365;// max one year !
    private static final Logger log = LogFactory.getLogger(ListOperation.class);

    //BLPOP
    public void blpop(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        this.block(request, request.getArgs(), e -> e.leftPop(), (o) -> {
            Bytes[] bytes = o.orElse(new Bytes[]{});
            byte[][] res = Arrays.stream(bytes).map(Bytes::getBytes).toArray(byte[][]::new);
            RedisOutputProtocol.writerMulti(request.getOutputStream(), res);
        });
    }

    //BRPOP
    public void brpop(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        this.block(request, request.getArgs(), e -> e.rightPop(), (o) -> {
            Bytes[] bytes = o.orElse(new Bytes[]{});
            byte[][] res = Arrays.stream(bytes).map(Bytes::getBytes).toArray(byte[][]::new);
            RedisOutputProtocol.writerMulti(request.getOutputStream(), res);
        });
    }

    private void block(
            RedisRequest request,
            ExpectRedisRequest[] args,
            Function<ListData, Optional<Bytes>> fetch, //
            ConsumerIO<Optional<Bytes[]>> done
    ) throws IOException {
        //ExpectRedisRequest[] args = request.getArgs();
        RedisDatabase db = request.getDatabase();
        long timeoutSeconds = args[args.length - 1].byteArray2long();
        if (timeoutSeconds <= 0) {
            timeoutSeconds = MAX_WAIT_SECONDS;
        }
        long stopTime = System.currentTimeMillis() + timeoutSeconds * 1000;
        long lost = stopTime - System.currentTimeMillis();
        if (lost > 0) {
            CountDownLatch latch = new CountDownLatch(1);
            Bytes[] res = Arrays.stream(args, 0, args.length - 1)
                    .map(e -> e.byteArray2hashKey()) // key
                    .map(e -> db.getOrCreate(e, ListData::new)) // create ListDate
                    .map(e -> e.blockPop(latch, fetch)) // add lock
                    .filter(Optional::isPresent).map(Optional::get)
                    .toArray(Bytes[]::new);
            request.setNext((r) -> {
                if (res.length <= 0) { // lock !
                    try {
                        latch.await(lost, TimeUnit.MILLISECONDS);
                        log.debug("Release {}", this);
                        ExpectRedisRequest[] na = Arrays.copyOf(args, args.length);
                        String next = (stopTime - System.currentTimeMillis()) / 1000 + "";
                        na[args.length - 1] = new ExpectRedisRequest(next.getBytes());
                        RedisRequest rq = new RedisRequest(r.getServer(), r.getChannel(), na);
                        rq.setNext(r.getNext());
                        request.getServer().getSchema().exec(rq);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        throw new RequestInterruptedException("List Operation Interrupted", e);
                    } finally {
                        for (int i = 1; i < args.length - 1; i++) {
                            HashKey key = args[i].byteArray2hashKey();
                            db.getOrCreate(key, ListData::new).removeCountDownLatch(latch);
                        }
                    }
                } else {
                    done.accept(Optional.of(res));
                }
            });
        } else {
            done.accept(Optional.empty());
        }
        // return res.stream().map(Bytes::getBytes).toArray(byte[][]::new);
    }

    //BRPOPLPUSH
    public void brpoplpush(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        //long start = System.currentTimeMillis();
        this.block(request, new ExpectRedisRequest[]{request.getArgs()[0], request.getArgs()[2]}, e -> e.rightPop(), (o) -> {
            HashKey hk = request.getArgs()[1].byteArray2hashKey();
            byte[] data = null;
            if (o.isPresent()) {
                data = o.get()[0].getBytes();
                ListData oc = request.getDatabase().getOrCreate(hk, ListData::new);
                oc.leftAdd(data);
            }
            //double using = (System.currentTimeMillis() - start) / 1000.0;
            //String val = BigDecimal.valueOf(using).setScale(3, BigDecimal.ROUND_HALF_DOWN).toPlainString();
            RedisOutputProtocol.writer(request.getOutputStream(), data);
        });
    }

    //LINDEX key index
    public void lindex(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<ListData> opt = getList(request);
        if (opt.isPresent()) {
            int index = Integer.parseInt(args[1].getByteArray2string());
            byte[][] bt = opt.get().range(index, index);
            RedisOutputProtocol.writer(request.getOutputStream(), bt.length == 0 ? null : bt[0]);
        } else {
            RedisOutputProtocol.writerNull(request.getOutputStream());
        }
    }

    //LINSERT
    public void linsert(RedisRequest request) throws IOException {
        request.expectArgumentsCount(4);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<ListData> opt = getList(request);
        if (opt.isPresent()) {
            boolean before = false;
            String key = args[1].getByteArray2string();
            if (key.equalsIgnoreCase("before")) {
                before = true;
            }
            byte[] pivot = args[2].getByteArray();
            byte[] val = args[3].getByteArray();
            boolean insert = opt.get().findAndOffsetInsert(pivot, before ? 0 : 1, val);
            int len = -1;
            if (insert) {
                len = opt.get().getData().size();
            }
            RedisOutputProtocol.writer(request.getOutputStream(), len);
        } else {
            RedisOutputProtocol.writer(request.getOutputStream(), 0);
        }
    }

    //LLEN
    public void llen(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<ListData> opt = getList(request);
        int size = opt.map(e -> e.getData().size()).orElse(0);
        RedisOutputProtocol.writer(request.getOutputStream(), size);
    }

    //LPOP
    public void lpop(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<ListData> opt = getList(request);
        Bytes data = null;
        if (opt.isPresent()) {
            Optional<Bytes> bytes = opt.get().leftPop();
            data = bytes.orElse(null);
        }
        RedisOutputProtocol.writer(request.getOutputStream(), data);
    }

    //LPUSH
    public void lpush(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        ListData data = this.getOrCreateList(request);
        ExpectRedisRequest[] args = request.getArgs();
        byte[][] bytes = Arrays.stream(args, 1, args.length)
                .map(ExpectRedisRequest::getByteArray)
                .toArray(byte[][]::new);
        data.leftAdd(bytes);
        RedisOutputProtocol.writer(request.getOutputStream(), data.getData().size());
    }

    //LPUSHX
    public void lpushx(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        Optional<ListData> opt = this.getList(request);
        if (opt.isPresent()) {
            ExpectRedisRequest[] args = request.getArgs();
            byte[][] bytes = Arrays.stream(args, 1, args.length)
                    .map(ExpectRedisRequest::getByteArray)
                    .toArray(byte[][]::new);
            opt.get().leftAdd(bytes);
        }
        RedisOutputProtocol.writer(request.getOutputStream(), opt.map(e -> e.getData().size()).orElse(0));
    }

    //LRANGE
    public void lrange(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        Optional<ListData> opt = this.getList(request);
        byte[][] range = new byte[0][];
        if (opt.isPresent()) {
            ExpectRedisRequest[] args = request.getArgs();
            range = opt.get().range(args[1].byteArray2int(), args[2].byteArray2int());
        }
        RedisOutputProtocol.writerMulti(request.getOutputStream(), range);
    }

    //LREM
    public void lrem(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        Optional<ListData> opt = this.getList(request);
        int len = 0;
        if (opt.isPresent()) {
            ExpectRedisRequest[] args = request.getArgs();
            len = opt.get().remove(args[1].byteArray2int(), args[2].getByteArray());
        }
        RedisOutputProtocol.writer(request.getOutputStream(), len);
    }

    //LSET
    // 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
    public void lset(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        ExpectRedisRequest[] args = request.getArgs();
        Optional<ListData> opt = getList(request);
        if (opt.isPresent()) {
            String index = args[1].getByteArray2string();
            try {
                opt.get().getData().set(Integer.parseInt(index), args[2].toBytes());
                RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
            } catch (ArrayIndexOutOfBoundsException e) {
                RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "Array Index Out Of Bounds");
            }
        } else {
            RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "NO LIST");
        }
    }

    //LTRIM
    public void ltrim(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        Optional<ListData> opt = getList(request);
        ExpectRedisRequest[] args = request.getArgs();
        opt.ifPresent(e -> {//
            e.trim(args[1].byteArray2int(), args[2].byteArray2int());
        });
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    //RPOP
    public void rpop(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        Optional<ListData> opt = this.getList(request);
        if (opt.isPresent()) {
            try {
                Optional<Bytes> rm = opt.get().rightPop();
                RedisOutputProtocol.writer(request.getOutputStream(), rm.orElse(null));
                return;
            } catch (IndexOutOfBoundsException i) {// ignore
            }
        }
        RedisOutputProtocol.writerNull(request.getOutputStream());
    }

    //RPOPLPUSH
    public void rpoplpush(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        HashKey target = request.getArgs()[1].byteArray2hashKey();
        Optional<ListData> opt = this.getList(request);
        Bytes data = opt.flatMap(e -> { // doing
            return e.pop2push(request.getDatabase(), target);
        }).orElse(null);
        RedisOutputProtocol.writer(request.getOutputStream(), data);
    }

    //RPUSH
    public void rpush(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(1);
        ListData list = this.getOrCreateList(request);
        ExpectRedisRequest[] args = request.getArgs();
        byte[][] bytes = Arrays.stream(args, 1, args.length)
                .map(ExpectRedisRequest::getByteArray)
                .toArray(byte[][]::new);
        list.rightAdd(bytes);
        RedisOutputProtocol.writer(request.getOutputStream(), list.getData().size());
    }

    //RPUSHX
    public void rpushx(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        Optional<ListData> opt = this.getList(request);
        ExpectRedisRequest[] args = request.getArgs();
        if (opt.isPresent()) {
            byte[] val = args[1].getByteArray();
            opt.get().rightAdd(val);
        }
        RedisOutputProtocol.writer(request.getOutputStream(), opt.map(e -> e.getData().size()).orElse(0));
    }

    private Optional<ListData> getList(RedisRequest request) {
        ExpectRedisRequest[] args = request.getArgs();
        HashKey key = new HashKey(args[0].getByteArray());
        RedisDatabase db = request.getDatabase();
        return db.get(key, ListData.class);
    }

    @NotNull
    private ListData getOrCreateList(RedisRequest request) {
        return this.getOrCreateList(request, 0);
    }

    @NotNull
    private ListData getOrCreateList(RedisRequest request, int index) {
        HashKey key = new HashKey(request.getArgs()[index].getByteArray());
        Optional<ListData> opt = this.getList(request);
        if (!opt.isPresent()) {
            ListData d = new ListData();
            ListData origin = request.getDatabase().putIfAbsent(key, d);
            opt = Optional.of(origin == null ? d : origin);
        }
        return opt.get();
    }
}
