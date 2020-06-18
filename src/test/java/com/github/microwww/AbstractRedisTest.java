package com.github.microwww;

import com.github.microwww.protocal.operation.Server;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetSocketAddress;

public abstract class AbstractRedisTest {

    protected static InetSocketAddress address;

    @BeforeClass
    public static void start() throws IOException {
        address = Server.startListener();
    }
}
