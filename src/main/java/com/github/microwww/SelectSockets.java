package com.github.microwww;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public abstract class SelectSockets {

    private ServerSocketChannel serverChannel;
    protected ServerSocket serverSocket;
    protected Selector selector;

    public Runnable config(String host, int port) throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverSocket = serverChannel.socket();
        selector = Selector.open();
        serverSocket.bind(new InetSocketAddress(host, port));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        return () -> {
            while (true) {
                try {
                    tryRun();
                } catch (IOException e) {
                    // TODO :: logger !
                    e.printStackTrace();
                }
            }
        };
    }

    public void tryRun() throws IOException {
        int n = selector.select(1000);
        if (n == 0) {
            return;
        }
        Iterator it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = (SelectionKey) it.next();
            try {
                if (key.isAcceptable()) {
                    ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    SocketChannel channel = server.accept();
                    this.acceptHandler(channel);
                }
                if (key.isReadable()) {
                    readableHandler(key);
                }
            } catch (IOException ex) { // 远程强制关闭了一个连接
                closeChannel(key);
            }
            it.remove();
        }
    }

    protected void registerChannel(SelectableChannel channel, int ops) throws IOException {
        if (channel == null) {
            return; // could happen
        }
        channel.configureBlocking(false);
        channel.register(selector, ops);
    }

    protected void readableHandler(SelectionKey key) throws IOException {
    }

    protected void acceptHandler(SocketChannel channel) throws IOException {
        registerChannel(channel, SelectionKey.OP_READ);
    }

    protected void closeChannel(SelectionKey key) throws IOException {
        System.out.println("Remote KILLED : " + key.channel());
        key.channel().close();
    }

    public ServerSocket getServerSocket() {
        if (serverChannel == null) {
            Thread.yield();
        }
        return serverSocket;
    }

}