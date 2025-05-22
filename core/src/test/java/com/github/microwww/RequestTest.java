package com.github.microwww;

import org.junit.Test;

import java.io.BufferedOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class RequestTest {

    // @Test
    public void test() throws InterruptedException {
        //while (true) {
        Thread th = new Thread(() -> {
            int i = 0;
            for (; i < Integer.MAX_VALUE; i++) {
                try {
                    Socket sk = new Socket();
                    sk.connect(new InetSocketAddress("localhost", 10421), 1000);
                    BufferedOutputStream out = new BufferedOutputStream(sk.getOutputStream());
                    InetSocketAddress cd = (InetSocketAddress) sk.getLocalSocketAddress();
                    System.out.println("IN port :" + cd.getPort());
                    for (; i < Integer.MAX_VALUE; i++) {
                        out.write(("test: " + i + " [" + cd.getPort() + "] ").getBytes());
                        out.flush();
                        Thread.sleep(1000);
                    }
                    sk.close();
                } catch (Exception e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }
            }
        });
        th.start();
        th.join();
        //Thread.sleep(50000);
        //}
    }

    @Test
    public void test1() {
    }
}
