package com.github.microwww.protocal.operation;

import com.github.microwww.RedisServer;

import java.io.IOException;
import java.net.InetSocketAddress;

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
        return (InetSocketAddress) server.getServerSocket().getLocalSocketAddress();
    }
}
