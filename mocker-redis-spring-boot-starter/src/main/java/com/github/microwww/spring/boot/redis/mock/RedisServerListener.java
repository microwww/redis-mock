package com.github.microwww.spring.boot.redis.mock;

import com.github.microwww.redis.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
@ConditionalOnProperty(prefix = "mocker.redis", value = "enable", havingValue = "true", matchIfMissing = true)
public class RedisServerListener {

    private static final Logger logger = LoggerFactory.getLogger(RedisServerListener.class);

    @Bean(destroyMethod = "close")
    public RedisServer mockRedisServer(RedisProperties redisProperties) throws IOException {
        RedisServer server = new RedisServer();
        server.listener(redisProperties.getHost(), redisProperties.getPort());
        logger.info("Mocker Redis start :: [{}:{}], set 'server.redis.host' to match it", redisProperties.getHost(), redisProperties.getPort());
        return server;
    }

}
