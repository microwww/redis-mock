package com.github.microwww;

import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class RedisServer {
    private static final int PORT = Protocol.DEFAULT_PORT;
    private static final String HOST = Protocol.DEFAULT_HOST;
    private static final int BUF_SIZE = 1024 * 8;
    private static final int TIMEOUT = 1000;

    public static void main(String[] args) {
        ServerSocketChannel channel = selector(new InetSocketAddress(HOST, 0));
        InetSocketAddress address = (InetSocketAddress) channel.socket().getLocalSocketAddress();
        System.out.println("Rdis run " + address.getHostName() + ":" + address.getPort());
    }

    public static ServerSocketChannel selector(InetSocketAddress address) {
        try (
                Selector selector = Selector.open();
                ServerSocketChannel ssc = ServerSocketChannel.open();
        ) {
            ssc.socket().bind(address);
            ssc.configureBlocking(false);
            ssc.register(selector, SelectionKey.OP_ACCEPT);
            Thread th = new Thread(() -> {
                while (true) {
                    accept(selector);
                }
            });
            th.setName("ServerSocketChannel");
            th.setDaemon(false);
            th.start();
            return ssc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void accept(Selector selector) {
        try {
            if (selector.select(TIMEOUT) == 0) {
                return;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
        while (iter.hasNext()) {
            SelectionKey key = iter.next();
            try {
                if (key.isAcceptable()) {
                    handleAccept(key);
                }
                if (key.isReadable()) {
                    handleRead(key);
                }
            } catch (Exception e) {
                e.printStackTrace(); // TODO :: logger
            } finally {
                iter.remove();
            }
        }
    }

    public static void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel ssChannel = (ServerSocketChannel) key.channel();
        SocketChannel sc = ssChannel.accept();
        sc.configureBlocking(false);
        sc.register(key.selector(), SelectionKey.OP_READ, ByteBuffer.allocateDirect(BUF_SIZE));
    }

    public static void handleRead(SelectionKey key) throws IOException {
        SocketChannel sc = (SocketChannel) key.channel();
        ByteBuffer buf = (ByteBuffer) key.attachment();
        long bytesRead = sc.read(buf);
        while (bytesRead > 0) {
            buf.flip();
            while (buf.hasRemaining()) {
                System.out.print((char) buf.get());
            }
            System.out.println();
            buf.clear();
            bytesRead = sc.read(buf);
        }
        if (bytesRead == -1) {
            sc.close();
        }
    }
}
