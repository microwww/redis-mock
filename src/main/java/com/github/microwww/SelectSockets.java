package com.github.microwww;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SelectSockets {

    private ServerSocketChannel serverChannel;
    protected ServerSocket serverSocket;
    protected Selector selector;
    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
    private ConcurrentHashMap<String, List<byte[]>> data = new ConcurrentHashMap();

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
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = getByteBuffer();
        while (true) {
            buffer.clear();
            int read = channel.read(buffer);
            if (read == -1) { // 远程正常关闭连接
                closeChannel(key);
            }
            if (read <= 0) {
                break;
            }
            buffer.flip();
            parsData(channel, buffer.asReadOnlyBuffer());
        }
    }

    protected void parsData(SocketChannel channel, ByteBuffer buffer) throws IOException {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        List<byte[]> list = Collections.synchronizedList(new LinkedList<>());
        String key = getKey(channel);
        list = data.putIfAbsent(key, list);
        list.add(bytes);
    }

    public String getKey(SocketChannel channel) throws IOException {
        InetSocketAddress address = (InetSocketAddress) channel.getRemoteAddress();
        return address.getHostName() + ":" + address.getPort();
    }

    protected void acceptHandler(SocketChannel channel) throws IOException {
        registerChannel(channel, SelectionKey.OP_READ);
    }

    protected void closeChannel(SelectionKey key) throws IOException {
        try {
            System.out.println("Remote KILLED : " + key.channel());
            key.channel().close();
        } finally {
            SocketChannel channel = (SocketChannel) key.channel();
            data.remove(getKey(channel));
        }
    }

    public ServerSocket getServerSocket() {
        if (serverChannel == null) {
            Thread.yield();
        }
        return serverSocket;
    }

    protected ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

}