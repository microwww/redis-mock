package com.github.microwww.redis.database;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

public class PubSub implements Closeable {
    /**
     * 当创建一个新的 `通道` 时的监听器, `psubscribe` 会创建一个监听者, 监听新建的`通道` 是否匹配对应的正则 (patten)
     */
    public final NewChannelNotify newChannelNotify = new NewChannelNotify();
    private Map<Bytes, MessageChannel> channels = new HashMap<>();

    public int publish(Bytes channel, Bytes message) {
        MessageChannel notify = this.getOrNewMessageNotify(channel);
        return notify.notify(message);
    }

    private MessageChannel getOrNewMessageNotify(Bytes channel) {
        MessageChannel mn = channels.get(channel);
        if (mn == null) {
            mn = new MessageChannel(channel);
            channels.put(channel, mn);
            this.newChannelNotify.notify(channel);
        }
        return mn;
    }

    public void subscribe(Bytes channel, Observer o) {
        this.getOrNewMessageNotify(channel).addObserver(o);
    }

    public void unsubscribe(Bytes channel, Observer o) {
        Observable obs = channels.get(channel);
        if (obs != null) {
            obs.deleteObserver(o);
        }
    }

    @Override
    public void close() throws IOException {
        channels.clear();
    }

    public static class MessageChannel extends Observable {
        private final Bytes channel;

        public MessageChannel(Bytes channel) {
            this.channel = channel;
        }

        public Bytes getChannel() {
            return channel;
        }

        public int notify(Bytes message) {
            this.setChanged();
            this.notifyObservers(message);
            return this.countObservers();
        }
    }

    public class NewChannelNotify extends Observable {

        public int notify(Bytes channel) {
            this.setChanged();
            this.notifyObservers(channel);
            return this.countObservers();
        }

        public void subscribe(Observer o) {
            newChannelNotify.addObserver(o);
        }

        public void unsubscribe(Observer o) {
            newChannelNotify.deleteObserver(o);
        }
    }
}
