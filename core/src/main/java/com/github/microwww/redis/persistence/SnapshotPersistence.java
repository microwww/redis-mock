package com.github.microwww.redis.persistence;

import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.database.AbstractValueData;
import com.github.microwww.redis.database.ByteData;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.GeoData;
import com.github.microwww.redis.database.HashData;
import com.github.microwww.redis.database.HashKey;
import com.github.microwww.redis.database.ListData;
import com.github.microwww.redis.database.Member;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.database.SetData;
import com.github.microwww.redis.database.SortedSetData;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SnapshotPersistence implements Closeable {

    private static final Logger log = LogFactory.getLogger(SnapshotPersistence.class);

    private static final byte[] MAGIC = new byte[]{'M', 'O', 'C', 'K', 'R', 'D', 'B', '1'};
    private static final int FILE_VERSION = 1;
    private static final int EOF_MARKER = 0xED0F0001;

    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_HASH = 2;
    private static final byte TYPE_LIST = 3;
    private static final byte TYPE_SET = 4;
    private static final byte TYPE_ZSET = 5;
    private static final byte TYPE_GEO = 6;

    private final RedisServer server;
    private final PersistenceConfig config;
    private final AtomicBoolean isSaving = new AtomicBoolean(false);
    private final AtomicLong lastSaveTime = new AtomicLong(System.currentTimeMillis());
    private final ExecutorService bgPool = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "REDIS-BGSAVE");
        thread.setDaemon(true);
        return thread;
    });
    private ScheduledExecutorService autoSaveScheduler;

    public SnapshotPersistence(RedisServer server) {
        this(server, new PersistenceConfig());
    }

    public SnapshotPersistence(RedisServer server, PersistenceConfig config) {
        this.server = server;
        this.config = config != null ? config : new PersistenceConfig();
        initAutoSave();
    }

    private void initAutoSave() {
        if (config.getAutoSaveIntervalMillis() > 0) {
            autoSaveScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "REDIS-AUTOSAVE");
                thread.setDaemon(true);
                return thread;
            });
            autoSaveScheduler.scheduleWithFixedDelay(() -> {
                try {
                    bgsave();
                } catch (Exception e) {
                    log.error("Auto save failed", e);
                }
            }, config.getAutoSaveIntervalMillis(), config.getAutoSaveIntervalMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public PersistenceConfig getConfig() {
        return config;
    }

    public long getLastSaveTime() {
        return lastSaveTime.get();
    }

    public boolean isSaving() {
        return isSaving.get();
    }

    public synchronized void save() throws IOException {
        save(config.getFile());
    }

    public synchronized void save(File targetFile) throws IOException {
        if (targetFile == null) {
            targetFile = new File(PersistenceConfig.DEFAULT_DUMP_FILE_NAME);
        }
        File parent = targetFile.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        File tempFile = new File(parent, targetFile.getName() + "." + UUID.randomUUID() + ".tmp");
        try {
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))) {
                writeSnapshot(out);
                out.flush();
            }

            try {
                Files.move(tempFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            lastSaveTime.set(System.currentTimeMillis());
            log.info("DB saved on disk at: {}", targetFile.getAbsolutePath());
        } finally {
            if (tempFile.exists()) {
                tempFile.delete();
            }
        }
    }

    public boolean bgsave() throws IOException {
        return bgsave(config.getFile());
    }

    public boolean bgsave(File targetFile) throws IOException {
        if (!isSaving.compareAndSet(false, true)) {
            return false;
        }
        bgPool.execute(() -> {
            try {
                save(targetFile);
            } catch (Exception e) {
                log.error("Async background save failed", e);
            } finally {
                isSaving.set(false);
            }
        });
        return true;
    }

    public synchronized boolean load() throws IOException {
        return load(config.getFile());
    }

    public synchronized boolean load(File sourceFile) throws IOException {
        if (sourceFile == null || !sourceFile.exists() || !sourceFile.isFile()) {
            return false;
        }

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(sourceFile)))) {
            readSnapshot(in);
            lastSaveTime.set(sourceFile.lastModified());
            log.info("DB loaded from disk: {}", sourceFile.getAbsolutePath());
            return true;
        }
    }

    private void writeSnapshot(DataOutputStream out) throws IOException {
        out.write(MAGIC);
        out.writeInt(FILE_VERSION);
        out.writeLong(System.currentTimeMillis());

        Schema schema = server.getSchema();
        int size = schema.getSize();

        List<Integer> activeDbIndices = new ArrayList<>();
        Map<Integer, List<Map.Entry<HashKey, AbstractValueData<?>>>> dbEntries = new HashMap<>();

        for (int i = 0; i < size; i++) {
            RedisDatabase db = schema.getRedisDatabases(i);
            Map<HashKey, AbstractValueData<?>> map = db.getUnmodifiableMap();
            List<Map.Entry<HashKey, AbstractValueData<?>>> validList = new ArrayList<>();
            for (Map.Entry<HashKey, AbstractValueData<?>> entry : map.entrySet()) {
                AbstractValueData<?> val = entry.getValue();
                if (val != null && !val.isExpired()) {
                    validList.add(entry);
                }
            }
            if (!validList.isEmpty()) {
                activeDbIndices.add(i);
                dbEntries.put(i, validList);
            }
        }

        out.writeInt(activeDbIndices.size());

        for (int dbIndex : activeDbIndices) {
            out.writeInt(dbIndex);
            List<Map.Entry<HashKey, AbstractValueData<?>>> list = dbEntries.get(dbIndex);
            out.writeInt(list.size());
            for (Map.Entry<HashKey, AbstractValueData<?>> entry : list) {
                HashKey key = entry.getKey();
                AbstractValueData<?> val = entry.getValue();

                byte[] keyBytes = key.getBytes();
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeLong(val.getExpire());

                writeValueData(out, val);
            }
        }

        out.writeInt(EOF_MARKER);
    }

    private void writeValueData(DataOutputStream out, AbstractValueData<?> val) throws IOException {
        if (val instanceof ByteData) {
            out.writeByte(TYPE_STRING);
            byte[] bytes = ((ByteData) val).getData();
            out.writeInt(bytes.length);
            out.write(bytes);
        } else if (val instanceof HashData) {
            out.writeByte(TYPE_HASH);
            Map<HashKey, Bytes> map = ((HashData) val).getData();
            out.writeInt(map.size());
            for (Map.Entry<HashKey, Bytes> entry : map.entrySet()) {
                byte[] fBytes = entry.getKey().getBytes();
                out.writeInt(fBytes.length);
                out.write(fBytes);
                byte[] vBytes = entry.getValue().getBytes();
                out.writeInt(vBytes.length);
                out.write(vBytes);
            }
        } else if (val instanceof ListData) {
            out.writeByte(TYPE_LIST);
            List<Bytes> list = ((ListData) val).getData();
            out.writeInt(list.size());
            for (Bytes b : list) {
                byte[] bBytes = b.getBytes();
                out.writeInt(bBytes.length);
                out.write(bBytes);
            }
        } else if (val instanceof SetData) {
            out.writeByte(TYPE_SET);
            Set<Bytes> set = ((SetData) val).getData();
            out.writeInt(set.size());
            for (Bytes b : set) {
                byte[] bBytes = b.getBytes();
                out.writeInt(bBytes.length);
                out.write(bBytes);
            }
        } else if (val instanceof SortedSetData) {
            out.writeByte(TYPE_ZSET);
            NavigableSet<Member> set = ((SortedSetData) val).getData();
            out.writeInt(set.size());
            for (Member m : set) {
                byte[] mBytes = m.getMember();
                out.writeInt(mBytes.length);
                out.write(mBytes);
                byte[] sBytes = m.getScore().toPlainString().getBytes(StandardCharsets.UTF_8);
                out.writeInt(sBytes.length);
                out.write(sBytes);
            }
        } else if (val instanceof GeoData) {
            out.writeByte(TYPE_GEO);
            Map<HashKey, double[]> positions = ((GeoData) val).getData();
            out.writeInt(positions.size());
            for (Map.Entry<HashKey, double[]> entry : positions.entrySet()) {
                byte[] mBytes = entry.getKey().getBytes();
                out.writeInt(mBytes.length);
                out.write(mBytes);
                double[] pos = entry.getValue();
                out.writeDouble(pos[0]);
                out.writeDouble(pos[1]);
            }
        } else {
            throw new IOException("Unsupported data type for persistence: " + val.getClass().getName());
        }
    }

    private void readSnapshot(DataInputStream in) throws IOException {
        byte[] magic = new byte[MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new IOException("Corrupted dump file: Invalid magic header");
        }

        int version = in.readInt();
        if (version != FILE_VERSION) {
            throw new IOException("Unsupported dump file version: " + version);
        }

        long saveTimestamp = in.readLong();
        int activeDbCount = in.readInt();

        Schema schema = server.getSchema();
        long now = System.currentTimeMillis();

        for (int i = 0; i < activeDbCount; i++) {
            int dbIndex = in.readInt();
            int keyCount = in.readInt();

            RedisDatabase db = schema.getRedisDatabases(dbIndex);

            for (int k = 0; k < keyCount; k++) {
                int keyLen = in.readInt();
                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);
                HashKey hashKey = new HashKey(keyBytes);

                long expire = in.readLong();
                byte type = in.readByte();

                AbstractValueData<?> valData = readValueData(in, type, expire);

                if (expire > 0 && expire <= now) {
                    continue;
                }

                if (valData != null) {
                    db.put(hashKey, valData);
                }
            }
        }

        int eof = in.readInt();
        if (eof != EOF_MARKER) {
            throw new IOException("Corrupted dump file: Missing or invalid EOF marker");
        }
    }

    private AbstractValueData<?> readValueData(DataInputStream in, byte type, long expire) throws IOException {
        if (type == TYPE_STRING) {
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            return new ByteData(bytes, expire);
        } else if (type == TYPE_HASH) {
            int count = in.readInt();
            Map<HashKey, Bytes> map = new ConcurrentHashMap<>();
            for (int i = 0; i < count; i++) {
                int fLen = in.readInt();
                byte[] fBytes = new byte[fLen];
                in.readFully(fBytes);
                int vLen = in.readInt();
                byte[] vBytes = new byte[vLen];
                in.readFully(vBytes);
                map.put(new HashKey(fBytes), new Bytes(vBytes));
            }
            return new HashData(map, expire);
        } else if (type == TYPE_LIST) {
            int count = in.readInt();
            List<Bytes> list = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int len = in.readInt();
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                list.add(new Bytes(bytes));
            }
            ListData listData = new ListData(list, 0);
            listData.setExpire(expire);
            return listData;
        } else if (type == TYPE_SET) {
            int count = in.readInt();
            Set<Bytes> set = new ConcurrentSkipListSet<>();
            for (int i = 0; i < count; i++) {
                int len = in.readInt();
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                set.add(new Bytes(bytes));
            }
            SetData setData = new SetData(set, 0);
            setData.setExpire(expire);
            return setData;
        } else if (type == TYPE_ZSET) {
            int count = in.readInt();
            SortedSetData sortedSetData = new SortedSetData();
            for (int i = 0; i < count; i++) {
                int mLen = in.readInt();
                byte[] mBytes = new byte[mLen];
                in.readFully(mBytes);
                int sLen = in.readInt();
                byte[] sBytes = new byte[sLen];
                in.readFully(sBytes);
                String scoreStr = new String(sBytes, StandardCharsets.UTF_8);
                Member member = new Member(mBytes, new BigDecimal(scoreStr));
                sortedSetData.addOrReplace(member);
            }
            sortedSetData.setExpire(expire);
            return sortedSetData;
        } else if (type == TYPE_GEO) {
            int count = in.readInt();
            GeoData geoData = new GeoData();
            for (int i = 0; i < count; i++) {
                int mLen = in.readInt();
                byte[] mBytes = new byte[mLen];
                in.readFully(mBytes);
                double lon = in.readDouble();
                double lat = in.readDouble();
                geoData.add(false, false, false, lon, lat, mBytes);
            }
            geoData.setExpire(expire);
            return geoData;
        } else {
            throw new IOException("Unknown type code: " + type);
        }
    }

    @Override
    public void close() {
        if (autoSaveScheduler != null) {
            autoSaveScheduler.shutdown();
        }
        bgPool.shutdown();
    }
}
