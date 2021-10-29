package com.github.microwww.redis;

import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.protocal.RequestSession;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.function.Consumer;

public class ChannelContext implements Closeable {
    private static final String PUB_SUB_N_KEY = ChannelContext.class.getName() + ".channels.names";
    private static final String PUB_SUB_P_KEY = ChannelContext.class.getName() + ".channels.pattens";

    private final CloseObservable listener = new CloseObservable();
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
    }

    public ChannelSessionHandler getChannelHandler() {
        return channelHandler;
    }

    void setChannelHandler(ChannelSessionHandler channelHandler) {
        this.channelHandler = channelHandler;
    }

    public SocketChannel getChannel() {
        return channel;
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
        listener.addObserver(os);
        return os;
    }

    public void removeCloseListener(CloseListener listener) {
        this.listener.deleteObserver(listener);
    }

    @Override
    public void close() throws IOException {
        listener.doClose();
        Map<Bytes, Observer> subscribes = subscribe.subscribes();
        if (subscribes != null) subscribes.clear(); // 多次 close 可能 NullPointerException
        this.sessions.close();
    }

    public class CloseObservable extends Observable {
        public void doClose() {
            this.setChanged();
            this.notifyObservers();
            this.deleteObservers();
        }
    }

    public class CloseListener implements Observer {
        private final Consumer<ChannelContext> notify;

        public CloseListener(Consumer<ChannelContext> notify) {
            this.notify = notify;
        }

        @Override
        public void update(Observable o, Object arg) {
            notify.accept(ChannelContext.this);
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
