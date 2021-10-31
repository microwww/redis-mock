package com.github.microwww.redis;

import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.exception.Run;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.RequestSession;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.util.StringUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.function.Consumer;

public class ChannelContext {
    private static final Logger log = LogFactory.getLogger(ChannelContext.class);
    private static final String PUB_SUB_N_KEY = ChannelContext.class.getName() + ".channels.names";
    private static final String PUB_SUB_P_KEY = ChannelContext.class.getName() + ".channels.pattens";

    private final String remoteHost;
    private final CloseObservable listeners = new CloseObservable();
    private final SocketChannel channel;
    private final RequestSession sessions;
    private final ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    private final Subscribe subscribe;
    private final Subscribe pattenSubscribe;
    private JedisOutputStream outputStream;
    private ChannelSessionHandler channelHandler;

    public ChannelContext(SocketChannel channel) {
        this.channel = channel;
        this.sessions = new RequestSession(channel);
        this.outputStream = new JedisOutputStream(new ChannelOutputStream(this.channel));
        subscribe = new Subscribe(PUB_SUB_N_KEY);
        pattenSubscribe = new Subscribe(PUB_SUB_P_KEY);
        this.remoteHost = StringUtil.remoteHost(channel);
    }

    public ChannelSessionHandler getChannelHandler() {
        return channelHandler;
    }

    void setChannelHandler(ChannelSessionHandler channelHandler) {
        this.channelHandler = channelHandler;
    }

    /**
     * invoke Channel.close will make Exception, so not any one can get it
     *
     * @return SocketChannel
     */
    protected SocketChannel getChannel() {
        return channel;
    }

    public void closeChannel() throws IOException {
        this.outputStream.close();
    }

    public RequestSession getSessions() {
        return sessions;
    }

    public ByteBuffer readChannel() throws IOException {
        buffer.clear();
        int read = channel.read(buffer);
        if (read < 0) {
            throw new IOException("EOF");
        }
        buffer.flip();
        return buffer.asReadOnlyBuffer();
    }

    public JedisOutputStream getOutputStream() {
        return outputStream;
    }

    public Subscribe getSubscribe() {
        return subscribe;
    }

    public Subscribe getPattenSubscribe() {
        return pattenSubscribe;
    }

    public CloseListener addCloseListener(Consumer<ChannelContext> notify) {
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

    /**
     *
     */
    protected void close() {
        Run.ignoreException(log, listeners::doClose);
        Run.ignoreException(log, () -> this.channelHandler.close(this));
        Run.ignoreException(log, () -> {
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
        private final Consumer<ChannelContext> notify;

        public CloseListener(Consumer<ChannelContext> notify) {
            this.notify = notify;
        }

        @Override
        public void update(Observable o, Object arg) {
            Run.ignoreException(log, () -> notify.accept(ChannelContext.this));
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
