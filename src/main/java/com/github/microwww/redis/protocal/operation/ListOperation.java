package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.ListData;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.Protocol;
import com.github.microwww.redis.util.IoConsumer;
import com.github.microwww.redis.util.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class ListOperation extends AbstractOperation {
    private static final long MAX_WAIT_SECONDS = 60 * 60 * 24 * 365;// max one year !
    private static final Logger log = LogFactory.getLogger(ListOperation.class);

    //BLPOP LIST1 LIST2 .. LISTN TIMEOUT
    public void blpop(RedisRequest request) throws IOException {
        blockPOP(request, ListData::leftPop);
    }

    //BRPOP
    public void brpop(RedisRequest request) throws IOException {
        blockPOP(request, ListData::rightPop);
    }

    public void pop_close(AddListener listener) throws IOException {
        // 2+ is [], 6 is null
        RedisRequest request = listener.request;
        RedisOutputProtocol.writerMulti(request.getOutputStream());
        request.getOutputStream().flush();
    }

    private void blockPOP(RedisRequest request, Function<ListData, Optional<Bytes>> pop) throws IOException {
        RequestParams[] args = request.getParams();
        long timeoutSeconds = args[args.length - 1].byteArray2long();
        blockPOP(request, timeoutSeconds, pop);
    }

    private AddListener blockPOP(RedisRequest request, long timeoutSeconds, Function<ListData, Optional<Bytes>> pop) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] args = request.getParams();
        AddListener listener = new AddListener(request, timeoutSeconds) {
            @Override
            public void changeRunning(long time) {
                try {
                    ListOperation.this.blockPOP(request, timeoutSeconds, pop);
                    request.getOutputStream().flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        for (int i = 0; i < args.length - 1; i++) {
            RequestParams param = request.getParams()[i];
            ListData list = this.getOrCreateList(request, i);
            listener.subscribe(list);
            Optional<Bytes> bytes = pop.apply(list);// list.blockLPOP(listener);
            if (bytes.isPresent()) {
                listener.clear();
                RedisOutputProtocol.writerMulti(request.getOutputStream(), param.getByteArray(), bytes.get().getBytes());
                return listener;
            }
        }
        if (timeoutSeconds > 0) {
            listener.timerSchedule(this::pop_close);
        }
        return listener;
    }

    public abstract class AddListener implements Observer {
        private final Timer timer = new Timer();
        RedisRequest request;
        List<ListData> listeners = new ArrayList<>();
        private boolean over = false;
        private final Date timeoutAT;

        public AddListener(RedisRequest request, long timeoutSeconds) {
            this.timeoutAT = new Date(System.currentTimeMillis() + timeoutSeconds * 1000);
            this.request = RedisRequest.warp(request, request.getCommand(), request.getParams());
        }

        @Override
        public void update(Observable o, Object arg) {
            if (!over) {
                long time = timeoutAT.getTime() - System.currentTimeMillis();
                if (time > 0) {
                    over = true;
                    request.getServer().getSchema().submit(() -> this.changeRunning(time));
                    this.clear();
                }
            }
        }

        public abstract void changeRunning(long remainTime);

        public void clear() {
            listeners.forEach(e -> e.unsubscribe(this));
            over = true;
        }

        public AddListener subscribe(ListData listener) {
            listeners.add(listener);
            listener.subscribe(this);
            return this;
        }

        /**
         * 防止多行程操作 使用  `Schema.submit`
         */
        public void timerSchedule(IoConsumer<AddListener> consumer) {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!over) try {
                        over = true;
                        consumer.accept(AddListener.this);
                    } catch (Exception e) {
                        throw new RuntimeException("Write time out `NULL` error!", e);
                    } finally {
                        AddListener.this.clear();
                    }
                }
            }, timeoutAT);
        }
    }

    //BRPOPLPUSH
    public void brpoplpush(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        long timeoutSeconds = request.getParams()[2].byteArray2long();
        brpoplpush(request, timeoutSeconds);
    }

    private void brpoplpush(RedisRequest request, long timeoutSeconds) throws IOException {
        AddListener listener = new AddListener(request, timeoutSeconds) {
            @Override
            public void changeRunning(long remainTime) {
                try {
                    brpoplpush(request, remainTime);
                    request.getOutputStream().flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        {
            ListData list = this.getOrCreateList(request, 0);
            listener.subscribe(list);
            Optional<Bytes> bytes = list.rightPop();
            if (bytes.isPresent()) {
                listener.clear();
                ListData target = this.getOrCreateList(request, 1);
                byte[] val = bytes.get().getBytes();
                target.leftAdd(val);
                RedisOutputProtocol.writer(request.getOutputStream(), val);
                return;
            }
        }
        if (timeoutSeconds > 0) {
            listener.timerSchedule(this::brpoplpush_close);
        }
    }

    public void brpoplpush_close(AddListener listener) {
        RedisRequest request = listener.request;
        try {
            RedisOutputProtocol.writer(request.getOutputStream(), new byte[]{});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //LINDEX key index
    public void lindex(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        RequestParams[] args = request.getParams();
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
        RequestParams[] args = request.getParams();
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
        RequestParams[] args = request.getParams();
        byte[][] bytes = Arrays.stream(args, 1, args.length)
                .map(RequestParams::getByteArray)
                .toArray(byte[][]::new);
        data.leftAdd(bytes);
        RedisOutputProtocol.writer(request.getOutputStream(), data.getData().size());
    }

    //LPUSHX
    public void lpushx(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        Optional<ListData> opt = this.getList(request);
        if (opt.isPresent()) {
            RequestParams[] args = request.getParams();
            byte[][] bytes = Arrays.stream(args, 1, args.length)
                    .map(RequestParams::getByteArray)
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
            RequestParams[] args = request.getParams();
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
            RequestParams[] args = request.getParams();
            len = opt.get().remove(args[1].byteArray2int(), args[2].getByteArray());
        }
        RedisOutputProtocol.writer(request.getOutputStream(), len);
    }

    //LSET
    // 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
    public void lset(RedisRequest request) throws IOException {
        request.expectArgumentsCount(3);
        RequestParams[] args = request.getParams();
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
        RequestParams[] args = request.getParams();
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
        HashKey target = request.getParams()[1].byteArray2hashKey();
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
        RequestParams[] args = request.getParams();
        byte[][] bytes = Arrays.stream(args, 1, args.length)
                .map(RequestParams::getByteArray)
                .toArray(byte[][]::new);
        list.rightAdd(bytes);
        RedisOutputProtocol.writer(request.getOutputStream(), list.getData().size());
    }

    //RPUSHX
    public void rpushx(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        Optional<ListData> opt = this.getList(request);
        RequestParams[] args = request.getParams();
        if (opt.isPresent()) {
            byte[] val = args[1].getByteArray();
            opt.get().rightAdd(val);
        }
        RedisOutputProtocol.writer(request.getOutputStream(), opt.map(e -> e.getData().size()).orElse(0));
    }

    private Optional<ListData> getList(RedisRequest request) {
        RequestParams[] args = request.getParams();
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
        HashKey key = new HashKey(request.getParams()[index].getByteArray());
        Optional<ListData> opt = this.getList(request);
        if (!opt.isPresent()) {
            ListData d = new ListData();
            ListData origin = request.getDatabase().putIfAbsent(key, d);
            opt = Optional.of(origin == null ? d : origin);
        }
        return opt.get();
    }
}
