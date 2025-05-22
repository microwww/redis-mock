package com.github.microwww.lettuce;

import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class TestHello extends AbstractLettuceTest {
    private static final Logger logger = LogFactory.getLogger(TestHello.class);

    @Test
    public void testHello() {
        RedisCommands<String, String> sync = redis.sync();
        String key = "key";
        String val = "Hello, Redis!";
        sync.set(key, val);
        String v = sync.get("key");
        Assert.assertEquals(v, val);
    }

    @Test(timeout = 1000)
    public void testV3pub_sub() throws Exception {
        String channel = "test-resp3";
        String msg = "test-channel: " + UUID.randomUUID();
        CountDownLatch count = new CountDownLatch(5);

        {// subscribe
            StatefulRedisPubSubConnection<String, String> cli = client.connectPubSub();
            cli.addListener(new RedisPubSubAdapter() {
                @Override
                public void message(Object channel, Object message) {
                    logger.info("Get message in `{}`: {}", channel, message);
                    Assert.assertEquals(msg, message);
                    count.countDown();
                }
            });
            RedisPubSubAsyncCommands<String, String> as1 = cli.async();
            as1.subscribe(channel).thenRun(() -> {
                count.countDown();
            });
        }
        {// p-subscribe
            StatefulRedisPubSubConnection<String, String> cli = client.connectPubSub();
            cli.addListener(new RedisPubSubAdapter() {

                @Override
                public void message(Object pattern, Object channel, Object message) {
                    logger.info("Get pattern `{}` message in `{}`: {}", pattern, channel, message);
                    Assert.assertEquals(msg, message);
                    count.countDown();
                }
            });
            RedisPubSubAsyncCommands<String, String> as1 = cli.async();
            as1.psubscribe(channel).thenRun(() -> {
                count.countDown();
            });
        }
        threads.execute(() -> {
            client.connectPubSub().sync().publish(channel, msg);
            count.countDown();
        });

        count.await();
    }

}
