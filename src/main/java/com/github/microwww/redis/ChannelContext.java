package com.github.microwww.redis;

import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.exception.Run;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RequestSession;
import com.github.microwww.redis.protocal.RespV2;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.StringUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.function.Consumer;

public class ChannelContext {
    private static final Logger log = LogFactory.getLogger(ChannelContext.class);
    private static final int DEF_CAPACITY = 8 * 1024;
    private static final String PUB_SUB_N_KEY = ChannelContext.class.getName() + ".channels.names";
    private static final String PUB_SUB_P_KEY = ChannelContext.class.getName() + ".channels.pattens";

    private final String remoteHost;
    private final CloseObservable listeners = new CloseObservable();

    /**
     * channel.close 可能无法获取关闭事件, 从而导致 ChannelContext 无法正确的回收 切忌!切忌!切忌!
     * <br />
     * invoke Channel.close will make some out of memory, so not any one can get it.
     */
    private final SocketChannel channel;
    private final RequestSession sessions;
    private ByteBuffer buffer = newByteBuffer();
    private final Subscribe subscribe;
    private final Subscribe pattenSubscribe;
    private RedisOutputProtocol protocol;
    private ChannelSessionHandler channelHandler;

    public ChannelContext(SocketChannel channel) {
        this.channel = channel;
        this.sessions = new RequestSession(channel);
        JedisOutputStream outputStream = new JedisOutputStream(new ChannelOutputStream(this.channel) {
            @Override
            public void close() {
                throw new UnsupportedOperationException("Not invoke it!, By `closeChannel`");
            }
        });
        protocol = new RespV2(outputStream);
        subscribe = new Subscribe(PUB_SUB_N_KEY);
        pattenSubscribe = new Subscribe(PUB_SUB_P_KEY);
        this.remoteHost = StringUtil.remoteHost(channel);
    }

    ChannelSessionHandler getChannelHandler() {
        return channelHandler;
    }

    void setChannelHandler(ChannelSessionHandler channelHandler) {
        this.channelHandler = channelHandler;
    }

    public ChannelContext assertChannel(SelectableChannel channel) {
        Assert.isTrue(this.channel == channel, "Channel not equal");
        return this;
    }

    public void closeChannel() throws IOException {
        try {
            this.protocol.getOut().flush();
        } catch (IOException ex) {// ignore
        } finally {
            this.close();
            this.channel.close();
        }
    }

    public RequestSession getSessions() {
        return sessions;
    }

    public ByteBuffer readChannel() throws IOException {
        int read = channel.read(buffer);
        if (read < 0) {
            throw new IOException("EOF");
        }
        buffer.flip();
        return buffer.asReadOnlyBuffer();
    }

    void readOver(ByteBuffer residue) throws IOException {
        if (residue.remaining() > 0) {
            if (residue.remaining() >= buffer.capacity()) {
                buffer = ByteBuffer.allocate(buffer.capacity() * 2);
            }
            buffer.clear();
            this.buffer.put(residue);
        } else if (buffer.capacity() == DEF_CAPACITY) {
            buffer.clear();
        } else {
            buffer = newByteBuffer();
        }
    }

    private static ByteBuffer newByteBuffer() {// netty is POOL
        return ByteBuffer.allocate(1 * 1024);
    }

    public RedisOutputProtocol getProtocol() {
        return protocol;
    }

    public void setProtocol(RedisOutputProtocol protocol) {
        this.protocol = protocol;
    }

    public Subscribe getSubscribe() {
        return subscribe;
    }

    public Subscribe getPattenSubscribe() {
        return pattenSubscribe;
    }

    public CloseListener addCloseListener(Consumer<ChannelContext> notify) {
        return addCloseListener0(() -> {
            notify.accept(this);
        });
    }

    public CloseListener addCloseListener0(Runnable notify) {
        CloseListener os = new CloseListener(notify);
        listeners.addObserver(os);
        log.debug("Add close listener, now {}", listeners.countObservers());
        return os;
    }

    public void removeCloseListener(CloseListener listener) {
        log.debug("Remove close listener, now {}", this.listeners.countObservers());
        this.listeners.deleteObserver(listener);
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public InetSocketAddress getRemoteAddress() throws IOException {
        return (InetSocketAddress) channel.getRemoteAddress();
    }

    protected void close() {
        log.warn("CLOSE {}", this.getRemoteHost());
        Run.ignoreException(log, listeners::doClose);
        Run.ignoreException(log, () -> this.channelHandler.close(this));
        Run.ignoreException(log, () -> {
            subscribe.removeSubscribe();
            Map<Bytes, Observer> subscribes = subscribe.subscribes();
            if (subscribes != null) subscribes.clear(); // 多次 close 可能 NullPointerException
        });
        this.sessions.close();
    }

    public class CloseObservable extends Observable {
        public void doClose() {
            this.setChanged();
            log.debug("Notify channel-context close listener - {}", listeners.countObservers());
            this.notifyObservers(ChannelContext.this);
        }
    }

    public class CloseListener implements Observer {
        private final Runnable notify;

        public CloseListener(Runnable notify) {
            this.notify = notify;
        }

        @Override
        public void update(Observable o, Object arg) {
            Run.ignoreException(log, () -> notify.run());
        }
    }

    public class Subscribe {
        private final String key;

        public Subscribe(String key) {
            this.key = key;
            ChannelContext.this.sessions.put(key, new LinkedHashMap<>());
        }

        public <T extends Observer> Map<Bytes, T> subscribeChannels() {
            return Collections.unmodifiableMap(subscribes());
        }

        private <T extends Observer> Map<Bytes, T> subscribes() {
            return (Map<Bytes, T>) ChannelContext.this.sessions.get(key);
        }

        public <T extends Observer> void addSubscribe(Bytes channel, T v) {
            subscribes().put(channel, v);
        }

        public <T extends Observer> Optional<T> getSubscribe(Bytes channel) {
            return Optional.ofNullable((T) subscribes().get(channel));
        }

        public <T extends Observer> Optional<T> removeSubscribe(Bytes channel) {
            return Optional.ofNullable((T) subscribes().remove(channel));
        }

        public void removeSubscribe() {
            subscribes().clear();
        }

    }
}
