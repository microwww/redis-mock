package com.github.microwww.redis;

import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.persistence.PersistenceConfig;
import com.github.microwww.redis.persistence.SnapshotPersistence;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.HalfPackException;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.message.RedisMessage;
import com.github.microwww.redis.protocal.message.Type;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.StringUtil;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer implements Closeable {
    public static final Logger log = LogFactory.getLogger(RedisServer.class);

    private final ExecutorService pool;
    private final SelectSockets sockets = new SelectSockets(this::handler);
    private Schema schema;
    private SnapshotPersistence persistence;

    public RedisServer() {
        this(5);
    }

    public RedisServer(int max) {
        this(Executors.newFixedThreadPool(max));
        Assert.isTrue(max > 2, "pool > 2");
    }

    public RedisServer(ExecutorService pool) {
        this.pool = pool;
    }

    public void configScheme(int size, AbstractOperation... operation) {
        Assert.isTrue(this.schema == null, "Server is running, you can not modify it, please invoke it before `getSchema`");
        this.schema = new Schema(size, operation);
    }

    public void listener(String host, int port) throws IOException {
        if (persistence != null && persistence.getConfig().isEnabled() && persistence.getConfig().isAutoLoad()) {
            try {
                persistence.load();
            } catch (Exception e) {
                log.error("Redis persistence auto load error", e);
            }
        }
        sockets.bind(host, port);
        // Runnable config = this.bind(host, port);

        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {//
                log.error("Thread runtime error {}", e);
            });
        }

        ServerSocket ss = sockets.getServerSocket();
        InetSocketAddress address = (InetSocketAddress) ss.getLocalSocketAddress();
        log.info("Redis server start @ {}:{}", address.getHostName(), "" + address.getPort());

        pool.execute(sockets::sync);
    }

    private RedisHandler handler(ChannelContext context) {
        return new RedisHandler(); // this
    }

    public Schema getSchema() {
        if (schema == null) {
            synchronized (this) {
                if (schema == null) {
                    schema = new Schema(Schema.DEFAULT_SCHEMA_SIZE);
                }
            }
        }
        return schema;
    }

    public SelectSockets getSockets() {
        return sockets;
    }

    public SnapshotPersistence getPersistence() {
        if (persistence == null) {
            synchronized (this) {
                if (persistence == null) {
                    persistence = new SnapshotPersistence(this);
                }
            }
        }
        return persistence;
    }

    public RedisServer setPersistence(SnapshotPersistence persistence) {
        this.persistence = persistence;
        return this;
    }

    public SnapshotPersistence enablePersistence(File file) {
        PersistenceConfig cfg = new PersistenceConfig(file).setEnabled(true);
        this.persistence = new SnapshotPersistence(this, cfg);
        return this.persistence;
    }

    public SnapshotPersistence enablePersistence(PersistenceConfig config) {
        this.persistence = new SnapshotPersistence(this, config);
        return this.persistence;
    }

    public void save() throws IOException {
        getPersistence().save();
    }

    public boolean bgsave() throws IOException {
        return getPersistence().bgsave();
    }

    public long getLastSaveTime() {
        return getPersistence().getLastSaveTime();
    }

    public class RedisHandler extends ChannelSessionHandler.Adaptor {

        @Override
        public void readableHandler(ChannelContext context, ByteBuffer buffer) throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Get a request: {}", context.getRemoteHost());
                StringUtil.loggerBuffer(log, buffer.asReadOnlyBuffer());
            }
            while (buffer.remaining() > 0) {
                int start = buffer.position();
                try {
                    RedisMessage redisMessage = Type.parseOne(buffer);
                    this.readableHandler(context, redisMessage);
                } catch (HalfPackException ex) {
                    ((java.nio.Buffer) buffer).position(start);
                    break;
                }
            }
        }

        private void readableHandler(ChannelContext context, RedisMessage message) throws IOException {
            RequestParams[] req = RequestParams.convert(message);
            RedisRequest redisRequest = new RedisRequest(RedisServer.this, context, req);
            log.debug("Ready [{}], request: {}", redisRequest.getCommand(), context.getRemoteHost());
            //Object o = context.getSessions().get(Transaction.class.getName());
            RedisServer.this.getSchema().execute(redisRequest);
            log.debug("Over  [{}], request: {}", redisRequest.getCommand(), context.getRemoteHost());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (persistence != null && persistence.getConfig().isEnabled() && persistence.getConfig().isSaveOnClose()) {
                try {
                    persistence.save();
                } catch (Exception e) {
                    log.error("Failed to save snapshot on close", e);
                }
            }
        } finally {
            if (persistence != null) {
                persistence.close();
            }
            try {
                this.sockets.close();
            } finally {
                try {
                    if (schema != null) schema.close();
                } finally {
                    pool.shutdown();
                }
            }
        }
    }
}
