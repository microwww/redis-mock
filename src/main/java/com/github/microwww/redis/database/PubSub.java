package com.github.microwww.redis.database;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

public class PubSub implements Closeable {
    private Map<Bytes, NotifyObservable> channels = new HashMap<>();

    public int publish(Bytes channel, Bytes message) {
        // channels.computeIfAbsent(channel, e -> new NotifyObservable(channel)).addObserver(o);
        NotifyObservable notify = channels.get(channel);
        if (notify != null) {
            return notify.notify(message);
        }
        return 0;
    }

    public void subscribe(Bytes channel, Observer o) {
        channels.computeIfAbsent(channel, e -> new NotifyObservable(channel)).addObserver(o);
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

    public static class NotifyObservable extends Observable {
        private final Bytes channel;

        public NotifyObservable(Bytes channel) {
            this.channel = channel;
        }

        public Bytes getChannel() {
            return channel;
        }

        public int notify(Bytes data) {
            this.setChanged();
            this.notifyObservers(data);
            return this.countObservers();
        }
    }
}
