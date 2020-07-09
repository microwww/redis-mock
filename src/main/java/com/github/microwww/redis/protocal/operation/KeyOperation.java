package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.*;
import com.github.microwww.redis.protocal.*;
import com.github.microwww.redis.util.Assert;
import redis.clients.jedis.Protocol;
import redis.clients.util.SafeEncoder;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class KeyOperation extends AbstractOperation {

    //EXPIRE
    public void expire(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Assert.isTrue(args.length == 2, "Must has tow arguments");
        HashKey key = args[0].byteArray2hashKey();
        int exp = args[1].byteArray2int();
        exp(request, key, exp);
    }

    private void exp(RedisRequest request, HashKey key, int exp) throws IOException {
        expMilliseconds(request, key, exp * 1000L + System.currentTimeMillis());
    }

    private void expMilliseconds(RedisRequest request, HashKey key, long exp) throws IOException {
        RedisDatabase db = request.getDatabase();
        db.setExpire(key, exp <= 0 ? 0 : exp);
        RedisOutputProtocol.writer(request.getOutputStream(), 1);
    }

    //DEL
    public void del(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(0);
        int count = 0;
        for (ExpectRedisRequest arg : request.getArgs()) {
            AbstractValueData<?> val = request.getDatabase().remove(arg.byteArray2hashKey());
            if (val != null) {
                count++;
            }
        }
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //DUMP
    //EXISTS
    public void exists(RedisRequest request) throws IOException {
        request.expectArgumentsCountBigger(0);
        ExpectRedisRequest[] args = request.getArgs();
        long count = Arrays.stream(args).map(e -> {
            HashKey key = e.byteArray2hashKey();
            return request.getDatabase().get(key);
        })//
                .filter(Optional::isPresent) //
                .count();
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //EXPIREAT
    public void expireat(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        int time = Integer.parseInt(request.getArgs()[1].getByteArray2string());
        long exp = time * 1000L;
        expMilliseconds(request, key, exp);
    }

    //KEYS
    public void keys(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        String patten = request.getArgs()[0].getByteArray2string();
        Pattern compile = ScanParams.toPattern(patten);

        List<byte[]> list = new ArrayList<>();
        for (HashKey s : request.getDatabase().getUnmodifiableMap().keySet()) {
            byte[] k = s.getBytes();
            String key = SafeEncoder.encode(k);
            if (compile.matcher(key).matches()) {
                list.add(k);
            }
        }
        RedisOutputProtocol.writerMulti(request.getOutputStream(), list.toArray(new byte[list.size()][]));
    }

    //MIGRATE
    //MOVE
    public void move(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        int index = request.getArgs()[1].byteArray2int();
        RedisDatabase db = request.getDatabase();
        RedisDatabase rd = request.getServer().getSchema().getRedisDatabases(index);
        Integer sync = db.sync(() -> {// db1 lock
            return rd.sync(() -> {// db2 lock
                Optional<AbstractValueData<?>> data = db.get(key);
                if (data.isPresent()) {
                    AbstractValueData<?> origin = rd.putIfAbsent(key, data.get());
                    if (origin == null) {
                        db.remove(key);
                        return 1;
                    }
                }
                return 0;
            });
        });
        RedisOutputProtocol.writer(request.getOutputStream(), sync.intValue());
    }

    //OBJECT
    //PERSIST
    public void persist(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        Optional<AbstractValueData<?>> opt = request.getDatabase().get(key);
        Integer re = opt.map((e) -> {//
            return request.getDatabase().sync(() -> {
                e.setExpire(AbstractValueData.NEVER_EXPIRE);
                return 1;
            });
        }).orElse(0);
        RedisOutputProtocol.writer(request.getOutputStream(), re.intValue());
    }

    //PEXPIRE
    public void pexpire(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        long time = Long.parseLong(request.getArgs()[1].getByteArray2string());
        expMilliseconds(request, key, System.currentTimeMillis() + time);
    }

    //PEXPIREAT
    public void pexpireat(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        long time = request.getArgs()[1].byteArray2long();
        expMilliseconds(request, key, time);
    }

    //PTTL
    public void pttl(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        Optional<AbstractValueData<?>> opt = request.getDatabase().get(key);
        Long time = opt.map(e -> {
            long ex = e.getExpire();
            if (ex > 0) {
                return ex - System.currentTimeMillis();
            }
            return -1L;
        }).orElse(-2L);
        RedisOutputProtocol.writer(request.getOutputStream(), time);
    }

    //RANDOMKEY
    public void randomkey(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        Map<HashKey, AbstractValueData<?>> map = request.getDatabase().getUnmodifiableMap();
        Set<HashKey> ks = map.keySet();
        if (ks.isEmpty()) {
            RedisOutputProtocol.writerNull(request.getOutputStream());
            return;
        }
        int v = ((int) (Math.random() * Integer.MAX_VALUE)) % ks.size();
        Iterator<HashKey> iterator = ks.iterator();
        HashKey val = null;
        for (int i = 0; i < v && iterator.hasNext(); i++) {
            val = iterator.next();
        }
        RedisOutputProtocol.writer(request.getOutputStream(), val == null ? null : val.getBytes());
    }

    //RENAME
    public void rename(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        HashKey target = request.getArgs()[1].byteArray2hashKey();
        boolean ok = this.rename(request, key, target, true);
        Assert.isTrue(ok, "overwrite not false");
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.name());
    }

    public boolean rename(RedisRequest request, HashKey key, HashKey target, boolean overwrite) throws RedisArgumentsException {
        if (key.equals(target)) {
            throw new RedisArgumentsException("key / newkey has same name");
        }
        RedisDatabase db = request.getDatabase();
        return db.sync(() -> {
            Optional<AbstractValueData<?>> remove = db.get(key);
            if (remove.isPresent()) {
                if (overwrite) {
                    db.put(target, remove.get());
                    return true;
                } else {
                    AbstractValueData<?> origin = db.putIfAbsent(target, remove.get());
                    return origin == null;
                }
            }
            throw new RedisArgumentsException("not find key");
        });
    }

    //RENAMENX
    public void renamenx(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        HashKey target = request.getArgs()[1].byteArray2hashKey();
        boolean ok = this.rename(request, key, target, false);
        RedisOutputProtocol.writer(request.getOutputStream(), ok ? 1 : 0);
    }

    //RESTORE
    //SORT
    public void sort(RedisRequest request) throws IOException {
        RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "Not support !");
    }

    //TTL
    public void ttl(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        Optional<AbstractValueData<?>> opt = request.getDatabase().get(key);
        Long time = opt.map(e -> {
            long ex = e.getExpire();
            if (ex > 0) {
                return (ex - System.currentTimeMillis()) / 1000;
            }
            return -1L;
        }).orElse(-2L);
        RedisOutputProtocol.writer(request.getOutputStream(), time.intValue());
    }

    //TYPE
    public void type(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        HashKey key = request.getArgs()[0].byteArray2hashKey();
        Optional<AbstractValueData<?>> opt = request.getDatabase().get(key);
        String type = opt.map(e -> e.getType()).orElse("none");
        RedisOutputProtocol.writer(request.getOutputStream(), type);
    }

    //SCAN
    public void scan(RedisRequest request) throws IOException {
        Iterator<HashKey> iterator = request.getDatabase().getUnmodifiableMap().keySet().iterator();
        new ScanIterator<HashKey>(request, 0).skip(iterator).continueWrite(iterator, e -> {//
            return e.getBytes();
        });
    }

    public static class Sort {

        public static Sort parseString(String[] args, int from, int len) {
            Assert.isTrue(from >= 0, " >= 0");
            Assert.isTrue(len >= 0, " >= 0");
            int max = Math.min(from + len, args.length);
            for (int i = from; i < max; i++) {
                String key = args[i];
                SortArgument of = SortArgument.valueOf(key.toUpperCase());
            }
            return new Sort();
        }
    }

    public enum SortArgument {
        BY(1), LIMIT(2), GET(1), ASC, DESC, ALPHA, STORE(1);

        private int count = 0;
        private List args = new ArrayList();

        private SortArgument() {
            this.count = 0;
        }

        private SortArgument(int count) {
            this.count = count;
        }

        public int getCount() {
            return count;
        }

    }

    public static class ScanParams {
        private Pattern pattern = Pattern.compile(".*");
        private int count = 10;

        public Pattern getPattern() {
            return pattern;
        }

        public void setPattern(Pattern pattern) {
            this.pattern = pattern;
        }

        public static Pattern toPattern(String pattern) {
            String patten = pattern.replaceAll(Pattern.quote("*"), ".*");
            patten = patten.replaceAll(Pattern.quote("?"), ".");
            return Pattern.compile(patten);
        }

        public int getCount() {
            return count;
        }

        public ScanParams setCount(int count) {
            Assert.isTrue(count > 0, "Count > 0");
            this.count = count;
            return this;
        }
    }

    public enum Scan {
        MATCH {
            @Override
            public int next(ScanParams params, ExpectRedisRequest[] args, int i) {
                params.pattern = ScanParams.toPattern(args[i + 1].getByteArray2string());
                return i + 1;
            }
        },
        COUNT {
            @Override
            public int next(ScanParams params, ExpectRedisRequest[] args, int i) {
                params.count = args[i + 1].byteArray2int();
                return i + 1;
            }
        };

        public abstract int next(ScanParams params, ExpectRedisRequest[] args, int i);
    }
}
