package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RedisServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

public class Server {

    public static RedisServer server;

    public static InetSocketAddress startListener() throws IOException {
        if (server == null) {
            synchronized (Server.class) {
                if (server == null) {
                    server = new RedisServer();
                    server.listener("localhost", 0);
                    Thread.yield();
                }
            }
        }
        return (InetSocketAddress) server.getSockets().getServerSocket().getLocalSocketAddress();
    }

    public static String[] random(int count) {
        String[] res = new String[count];
        for (int i = 0; i < count; i++) {
            res[i] = i + "--" + UUID.randomUUID().toString().replaceAll("-", "");
        }
        return res;
    }
}
