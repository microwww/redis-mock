package com.github.microwww.redis.protocal.message;

import com.github.microwww.redis.util.Assert;

import java.util.HashMap;
import java.util.Map;

public class MapMessage extends AbstractCollectionMessage {

    public MapMessage(Type prefix, RedisMessage[] bytes) {
        super(prefix, bytes);
    }

    public Map<RedisMessage, RedisMessage> map() {
        RedisMessage[] messages = getRedisMessages();
        int len = messages.length & 1;
        Assert.isTrue(len == 0, "map length 2x");
        Map<RedisMessage, RedisMessage> map = new HashMap<>();
        for (int i = 1; i < messages.length; i += 2) {
            map.put(messages[i - 1], messages[i]);
        }
        return map;
    }

    public Map<String, RedisMessage> mapString() {
        RedisMessage[] messages = getRedisMessages();
        int len = messages.length & 1;
        Assert.isTrue(len == 0, "map length 2x");
        Map<String, RedisMessage> map = new HashMap<>();
        for (int i = 1; i < messages.length; i += 2) {
            map.put(messages[i - 1].toString(), messages[i]);
        }
        return map;
    }
}
