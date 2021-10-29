package com.github.microwww.redis.database;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

public class PubSub implements Closeable {
    public final NewChannelNotify newChannelNotify = new NewChannelNotify();
    private Map<Bytes, MessageNotify> channels = new HashMap<>();

    public int publish(Bytes channel, Bytes message) {
        // channels.computeIfAbsent(channel, e -> new NotifyObservable(channel)).addObserver(o);
        MessageNotify notify = channels.get(channel);
        if (notify != null) {
            return notify.notify(message);
        }
        return 0;
    }

    public void subscribe(Bytes channel, Observer o) {
        channels.computeIfAbsent(channel, e -> new MessageNotify(channel)).addObserver(o);
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

    public class MessageNotify extends Observable {
        private final Bytes channel;

        public MessageNotify(Bytes channel) {
            this.channel = channel;
            // notify
            newChannelNotify.notify(channel);
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
