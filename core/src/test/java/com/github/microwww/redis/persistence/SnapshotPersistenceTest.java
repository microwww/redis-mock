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
import com.github.microwww.redis.database.SetData;
import com.github.microwww.redis.database.SortedSetData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

public class SnapshotPersistenceTest {

    private File tempDumpFile;

    @Before
    public void setup() throws IOException {
        tempDumpFile = File.createTempFile("redis-mock-dump-", ".rdb");
        if (tempDumpFile.exists()) {
            tempDumpFile.delete();
        }
    }

    @After
    public void cleanup() {
        if (tempDumpFile != null && tempDumpFile.exists()) {
            tempDumpFile.delete();
        }
    }

    @Test
    public void testSaveAndLoadAllTypes() throws IOException {
        RedisServer server1 = new RedisServer();
        try {
            RedisDatabase db0 = server1.getSchema().getRedisDatabases(0);

            // 1. String
            db0.put(new HashKey("string-key".getBytes(StandardCharsets.UTF_8)),
                    new ByteData("hello-world".getBytes(StandardCharsets.UTF_8), AbstractValueData.NEVER_EXPIRE));

            // 2. Hash
            HashData hashData = new HashData();
            hashData.put(new HashKey("f1".getBytes(StandardCharsets.UTF_8)), "v1".getBytes(StandardCharsets.UTF_8));
            hashData.put(new HashKey("f2".getBytes(StandardCharsets.UTF_8)), "v2".getBytes(StandardCharsets.UTF_8));
            db0.put(new HashKey("hash-key".getBytes(StandardCharsets.UTF_8)), hashData);

            // 3. List
            ListData listData = new ListData();
            listData.rightAdd("l1".getBytes(StandardCharsets.UTF_8), "l2".getBytes(StandardCharsets.UTF_8));
            db0.put(new HashKey("list-key".getBytes(StandardCharsets.UTF_8)), listData);

            // 4. Set
            SetData setData = new SetData();
            setData.add(new Bytes("s1".getBytes(StandardCharsets.UTF_8)), new Bytes("s2".getBytes(StandardCharsets.UTF_8)));
            db0.put(new HashKey("set-key".getBytes(StandardCharsets.UTF_8)), setData);

            // 5. SortedSet
            SortedSetData sortedSetData = new SortedSetData();
            sortedSetData.addOrReplace(
                    new Member("m1".getBytes(StandardCharsets.UTF_8), new BigDecimal("10.5")),
                    new Member("m2".getBytes(StandardCharsets.UTF_8), new BigDecimal("20.1"))
            );
            db0.put(new HashKey("zset-key".getBytes(StandardCharsets.UTF_8)), sortedSetData);

            // 6. Geo
            GeoData geoData = new GeoData();
            geoData.add(false, false, false, 116.40, 39.90, "beijing".getBytes(StandardCharsets.UTF_8));
            geoData.add(false, false, false, 121.47, 31.23, "shanghai".getBytes(StandardCharsets.UTF_8));
            db0.put(new HashKey("geo-key".getBytes(StandardCharsets.UTF_8)), geoData);

            // 7. db1 multiple databases
            RedisDatabase db1 = server1.getSchema().getRedisDatabases(1);
            db1.put(new HashKey("db1-key".getBytes(StandardCharsets.UTF_8)),
                    new ByteData("db1-val".getBytes(StandardCharsets.UTF_8), AbstractValueData.NEVER_EXPIRE));

            SnapshotPersistence persistence1 = new SnapshotPersistence(server1, new PersistenceConfig(tempDumpFile));
            persistence1.save();
            assertTrue(tempDumpFile.exists());
            assertTrue(tempDumpFile.length() > 0);
        } finally {
            server1.close();
        }

        // Create a new server2 and load from the dump file
        RedisServer server2 = new RedisServer();
        try {
            SnapshotPersistence persistence2 = new SnapshotPersistence(server2, new PersistenceConfig(tempDumpFile));
            boolean loaded = persistence2.load();
            assertTrue(loaded);

            RedisDatabase db0 = server2.getSchema().getRedisDatabases(0);

            // Verify String
            Optional<ByteData> stringVal = db0.get(new HashKey("string-key".getBytes(StandardCharsets.UTF_8)), ByteData.class);
            assertTrue(stringVal.isPresent());
            assertEquals("hello-world", new String(stringVal.get().getData(), StandardCharsets.UTF_8));

            // Verify Hash
            Optional<HashData> hashVal = db0.get(new HashKey("hash-key".getBytes(StandardCharsets.UTF_8)), HashData.class);
            assertTrue(hashVal.isPresent());
            Map<HashKey, Bytes> hMap = hashVal.get().getData();
            assertEquals("v1", hMap.get(new HashKey("f1".getBytes(StandardCharsets.UTF_8))).toString());
            assertEquals("v2", hMap.get(new HashKey("f2".getBytes(StandardCharsets.UTF_8))).toString());

            // Verify List
            Optional<ListData> listVal = db0.get(new HashKey("list-key".getBytes(StandardCharsets.UTF_8)), ListData.class);
            assertTrue(listVal.isPresent());
            List<Bytes> lList = listVal.get().getData();
            assertEquals(2, lList.size());
            assertEquals("l1", lList.get(0).toString());
            assertEquals("l2", lList.get(1).toString());

            // Verify Set
            Optional<SetData> setVal = db0.get(new HashKey("set-key".getBytes(StandardCharsets.UTF_8)), SetData.class);
            assertTrue(setVal.isPresent());
            Set<Bytes> sSet = setVal.get().getData();
            assertEquals(2, sSet.size());
            assertTrue(sSet.contains(new Bytes("s1".getBytes(StandardCharsets.UTF_8))));

            // Verify SortedSet
            Optional<SortedSetData> zsetVal = db0.get(new HashKey("zset-key".getBytes(StandardCharsets.UTF_8)), SortedSetData.class);
            assertTrue(zsetVal.isPresent());
            assertEquals(2, zsetVal.get().getData().size());

            // Verify Geo
            Optional<GeoData> geoVal = db0.get(new HashKey("geo-key".getBytes(StandardCharsets.UTF_8)), GeoData.class);
            assertTrue(geoVal.isPresent());
            Optional<double[]> bjPos = geoVal.get().getPosition(new HashKey("beijing".getBytes(StandardCharsets.UTF_8)));
            assertTrue(bjPos.isPresent());
            assertEquals(116.40, bjPos.get()[0], 0.001);
            assertEquals(39.90, bjPos.get()[1], 0.001);

            // Verify db1
            RedisDatabase db1 = server2.getSchema().getRedisDatabases(1);
            Optional<ByteData> db1Val = db1.get(new HashKey("db1-key".getBytes(StandardCharsets.UTF_8)), ByteData.class);
            assertTrue(db1Val.isPresent());
            assertEquals("db1-val", new String(db1Val.get().getData(), StandardCharsets.UTF_8));
        } finally {
            server2.close();
        }
    }

    @Test
    public void testExpiredKeysNotLoaded() throws IOException, InterruptedException {
        RedisServer server1 = new RedisServer();
        try {
            RedisDatabase db = server1.getSchema().getRedisDatabases(0);
            ByteData expiring = new ByteData("temp-value".getBytes(StandardCharsets.UTF_8), System.currentTimeMillis() + 100);
            db.put(new HashKey("temp-key".getBytes(StandardCharsets.UTF_8)), expiring);

            ByteData permanent = new ByteData("perm-value".getBytes(StandardCharsets.UTF_8), AbstractValueData.NEVER_EXPIRE);
            db.put(new HashKey("perm-key".getBytes(StandardCharsets.UTF_8)), permanent);

            SnapshotPersistence persistence1 = new SnapshotPersistence(server1, new PersistenceConfig(tempDumpFile));
            persistence1.save();

            // Wait for key expiration
            Thread.sleep(150);
        } finally {
            server1.close();
        }

        RedisServer server2 = new RedisServer();
        try {
            SnapshotPersistence persistence2 = new SnapshotPersistence(server2, new PersistenceConfig(tempDumpFile));
            persistence2.load();

            RedisDatabase db = server2.getSchema().getRedisDatabases(0);
            Optional<ByteData> tempVal = db.get(new HashKey("temp-key".getBytes(StandardCharsets.UTF_8)), ByteData.class);
            assertFalse(tempVal.isPresent());

            Optional<ByteData> permVal = db.get(new HashKey("perm-key".getBytes(StandardCharsets.UTF_8)), ByteData.class);
            assertTrue(permVal.isPresent());
            assertEquals("perm-value", new String(permVal.get().getData(), StandardCharsets.UTF_8));
        } finally {
            server2.close();
        }
    }

    @Test
    public void testAutoSaveOnCloseAndAutoLoadOnListener() throws IOException {
        RedisServer server1 = new RedisServer();
        server1.enablePersistence(new PersistenceConfig(tempDumpFile).setEnabled(true).setAutoLoad(true).setSaveOnClose(true));
        try {
            server1.listener("127.0.0.1", 0);
            server1.getSchema().getRedisDatabases(0).put(
                    new HashKey("auto-key".getBytes(StandardCharsets.UTF_8)),
                    new ByteData("auto-val".getBytes(StandardCharsets.UTF_8), AbstractValueData.NEVER_EXPIRE)
            );
        } finally {
            server1.close();
        }

        assertTrue(tempDumpFile.exists());

        RedisServer server2 = new RedisServer();
        server2.enablePersistence(new PersistenceConfig(tempDumpFile).setEnabled(true).setAutoLoad(true).setSaveOnClose(true));
        try {
            server2.listener("127.0.0.1", 0);
            Optional<ByteData> val = server2.getSchema().getRedisDatabases(0).get(
                    new HashKey("auto-key".getBytes(StandardCharsets.UTF_8)),
                    ByteData.class
            );
            assertTrue(val.isPresent());
            assertEquals("auto-val", new String(val.get().getData(), StandardCharsets.UTF_8));
        } finally {
            server2.close();
        }
    }
}
