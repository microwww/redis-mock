package com.github.microwww;

import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class SelectSocketsThreadPoolTest {

    @Test
    public void testPark() throws Exception {
        StringBuffer res = new StringBuffer();
        Executor pool = Executors.newFixedThreadPool(5);
        SelectSocketsThreadPool sst = new SelectSocketsThreadPool(pool) {
            @Override
            protected void readChannel(SocketChannel channel, AwaitRead lock) throws IOException {
                ByteBuffer bf = ByteBuffer.allocate(1024);
                while (true) {
                    bf.clear();
                    int read = channel.read(bf);
                    if (read < 0) { // 远程正常关闭连接
                        return;
                    }
                    if (read == 0) {
                        lock.park();
                        continue;
                    }
                    bf.flip();
                    byte[] bt = new byte[bf.remaining()];
                    bf.get(bt);
                    String str = new String(bt, "utf8");
                    res.append(str);
                    System.out.println(res.toString());
                }
            }
        };
        Runnable run = sst.config("localhost", 0);
        pool.execute(run);
        InetSocketAddress ss = (InetSocketAddress) sst.getServerSocket().getLocalSocketAddress();
        Thread.yield();
        {
            Socket sk = new Socket();
            sk.connect(new InetSocketAddress(ss.getHostName(), ss.getPort()), 1000);
            OutputStream out = sk.getOutputStream();
            String val = "abc";
            for (int i = 0; i < 10; i++) {
                out.write(val.getBytes());
                out.flush();
                Thread.sleep(100);
            }
            sk.close();
            Thread.sleep(100);
            assertEquals(val.length() * 10, res.toString().length());
        }
    }

}