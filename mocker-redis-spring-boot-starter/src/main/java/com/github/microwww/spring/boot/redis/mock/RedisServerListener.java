package com.github.microwww.spring.boot.redis.mock;

import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.persistence.PersistenceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@ConditionalOnProperty(prefix = "mocker.redis", value = "enable", havingValue = "true", matchIfMissing = true)
public class RedisServerListener {

    private static final Logger logger = LoggerFactory.getLogger(RedisServerListener.class);
    private static RedisServer server; // 运行多个单元测试的时候, 会启动多个 ApplicationContext, 导致多次端口绑定, 所以此处缓存
    private static AtomicInteger count = new AtomicInteger(0);

    @Value("${mocker.redis.persistence.enable:false}")
    private boolean persistenceEnable;

    @Value("${mocker.redis.persistence.file:dump.rdb}")
    private String persistenceFile;

    @Value("${mocker.redis.persistence.save-on-close:true}")
    private boolean saveOnClose;

    @Value("${mocker.redis.persistence.auto-load:true}")
    private boolean autoLoad;

    @Value("${mocker.redis.persistence.auto-save-interval-ms:0}")
    private long autoSaveIntervalMs;

    @Bean(destroyMethod = "close")
    public RedisServer mockRedisServer(RedisProperties redisProperties) throws IOException {
        if (server == null) synchronized (RedisServerListener.class) {
            if (server == null) {
                RedisServer server = new RedisServer() {
                    @Override
                    public void close() throws IOException {
                        if (count.decrementAndGet() <= 0) {
                            logger.info("Mocker Redis will STOP !");
                            super.close();
                        }
                    }
                };
                if (persistenceEnable) {
                    PersistenceConfig cfg = new PersistenceConfig(new File(persistenceFile))
                            .setEnabled(true)
                            .setAutoLoad(autoLoad)
                            .setSaveOnClose(saveOnClose)
                            .setAutoSaveIntervalMillis(autoSaveIntervalMs);
                    server.enablePersistence(cfg);
                    logger.info("Mocker Redis persistence enabled, file: {}", persistenceFile);
                }
                server.listener(redisProperties.getHost(), redisProperties.getPort());
                this.server = server;
            }
        }
        count.incrementAndGet();
        logger.info("Mocker Redis start :: [{}:{}], set 'server.redis.host' to match it", redisProperties.getHost(), redisProperties.getPort());
        return server;
    }

}
