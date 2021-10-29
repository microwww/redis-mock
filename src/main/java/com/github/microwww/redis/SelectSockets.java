package com.github.microwww.redis;

import com.github.microwww.redis.exception.Run;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.util.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class SelectSockets implements Closeable {
    private static final Logger logger = LogFactory.getLogger(SelectSockets.class);

    private ServerSocketChannel serverChannel;
    protected ServerSocket serverSocket;
    protected Selector selector;
    private AtomicBoolean close = new AtomicBoolean();
    private Set<ChannelContext> clients = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Function<ChannelContext, ChannelSessionHandler> factory;

    public SelectSockets(Function<ChannelContext, ChannelSessionHandler> factory) {
        Assert.isTrue(factory != null, "Not null");
        this.factory = factory;
    }

    public SelectSockets bind(String host, int port) throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverSocket = serverChannel.socket();
        selector = Selector.open();
        serverSocket.bind(new InetSocketAddress(host, port));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        return this;
    }

    /**
     * will block !
     */
    public void sync() {
        Thread.currentThread().setName("SELECT-IO");
        while (true) {
            if (close.get()) break;
            Run.ignoreException(logger, () -> {// start LISTENER
                this.tryRun();
            });
        }
        Run.ignoreException(logger, this::close);
    }

    private void tryRun() throws IOException {
        int n = selector.select();
        if (n <= 0) {
            return;
        }
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            try {
                if (key.isValid()) { // CancelledKeyException
                    if (key.isAcceptable()) {
                        ServerSocketChannel server = (ServerSocketChannel) key.channel();
                        SocketChannel channel = server.accept();
                        ChannelContext ctx = this.registerChannel(channel, SelectionKey.OP_READ);
                        ChannelSessionHandler channelHandler = factory.apply(ctx);
                        clients.add(ctx);
                        ctx.addCloseListener(clients::remove);
                        ctx.setChannelHandler(channelHandler);
                        try {
                            channelHandler.registerHandler(ctx);
                        } catch (Exception ex) {
                            channelHandler.exception(ctx, ex);
                        }
                    }
                    if (key.isReadable()) {
                        ChannelContext ctx = (ChannelContext) key.attachment();
                        Assert.isTrue(key.channel() == ctx.getChannel(), "-");
                        ByteBuffer read = ctx.readChannel();
                        ChannelSessionHandler channelHandler = ctx.getChannelHandler();
                        try {
                            channelHandler.readableHandler(ctx, read);
                        } catch (Exception ex) {
                            channelHandler.exception(ctx, ex);
                        }
                    }
                }
            } catch (IOException ex) { // 远程强制关闭了一个连接
                logger.debug("close client");
                try {
                    ChannelContext attachment = (ChannelContext) key.attachment();
                    try {
                        attachment.getChannelHandler().close(attachment);
                    } finally {
                        attachment.close();
                    }
                } finally {
                    closeChannel(key);
                }
            } finally {
                it.remove();
            }
        }
    }

    private ChannelContext registerChannel(SelectableChannel channel, int ops) throws IOException {
        if (channel == null) {
            return null;
        }
        channel.configureBlocking(false);
        ChannelContext ctx = new ChannelContext((SocketChannel) channel);
        channel.register(selector, ops, ctx);
        return ctx;
    }

    private void closeChannel(SelectionKey key) throws IOException {
        SelectableChannel channel = key.channel();
        if (channel instanceof SocketChannel) {
            this.closeChannel((SocketChannel) channel);
        }
        key.cancel();
    }

    public void closeChannel(SocketChannel channel) throws IOException {
        try {
            InetSocketAddress add = (InetSocketAddress) channel.getRemoteAddress();
            logger.info("Remote client {}:{} is closed", add.getHostName(), add.getPort());
        } catch (Exception e) {
            logger.info("Remote channel {} , is closed", channel);
        } finally {
            channel.close();
        }
    }

    public ServerSocket getServerSocket() {
        if (serverChannel == null) {
            Thread.yield();
        }
        return serverSocket;
    }

    public void stop() {
        if (close.get()) {
            return;
        }
        close.set(true);
    }

    public boolean isClose() {
        return close.get();
    }

    @Override
    public void close() throws IOException {
        close.set(true);
        try {
            if (selector != null)
                selector.close();
        } finally {
            if (serverSocket != null)
                serverSocket.close();
        }
    }

    public Set<ChannelContext> getClients() {
        return Collections.unmodifiableSet(clients);
    }
}