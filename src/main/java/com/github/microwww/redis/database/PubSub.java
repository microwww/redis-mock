package com.github.microwww.redis.database;

import com.github.microwww.redis.protocal.operation.PubSubOperation;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

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

    private synchronized MessageChannel getOrNewMessageNotify(Bytes channel) {
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

    public synchronized Map<Bytes, MessageChannel> getChannels() {
        return Collections.unmodifiableMap(channels);
    }

    @Override
    public void close() throws IOException {
        channels.clear();
    }

    public static class MessageChannel extends Observable {
        private final Bytes channel;
        private int numsub = 0; // not contain `patten`

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

        @Override
        public void addObserver(Observer o) {
            int i = this.countObservers();
            super.addObserver(o);
            if (this.countObservers() != i) {
                if (!isPatten(o)) {
                    numsub++;
                }
            }
        }

        @Override
        public void deleteObserver(Observer o) {
            int i = this.countObservers();
            super.deleteObserver(o);
            if (this.countObservers() != i) {
                if (!isPatten(o)) {
                    numsub++;
                }
            }
        }

        @Override
        public synchronized void deleteObservers() {
            super.deleteObservers();
            numsub = 0;
        }

        public int getNumsub() {
            return numsub;
        }

        public boolean isActive() {
            return this.getNumsub() > 0;
        }
    }

    public static boolean isPatten(Observer observer) {
        if (observer instanceof PubSubOperation.ChannelMessageListener) {
            PubSubOperation.ChannelMessageListener pc = ((PubSubOperation.ChannelMessageListener) observer);
            return pc.getPatten().isPresent();
        }
        return false;
    }

    public static Optional<Bytes> getNewPatten(Observer observer) {
        if (observer instanceof PubSubOperation.NewChannelListener) {
            PubSubOperation.NewChannelListener pc = ((PubSubOperation.NewChannelListener) observer);
            return Optional.of(pc.getPatten());
        }
        return Optional.empty();
    }

    public class NewChannelNotify extends Observable {
        private List<Bytes> pattens = new ArrayList<>();

        public int notify(Bytes channel) {
            this.setChanged();
            this.notifyObservers(channel);
            return this.countObservers();
        }

        public void subscribe(Observer o) {
            newChannelNotify.addObserver(o);
            // 已存在的历史 直接通知
            channels.values().forEach(e -> {
                o.update(this, e.channel);
            });
        }

        public void unsubscribe(Observer o) {
            newChannelNotify.deleteObserver(o);
        }

        @Override
        public synchronized void addObserver(Observer o) {
            int i = this.countObservers();
            super.addObserver(o);
            if (this.countObservers() != i) {
                getNewPatten(o).ifPresent(pattens::add);
            }
        }

        @Override
        public synchronized void deleteObserver(Observer o) {
            int i = this.countObservers();
            super.deleteObserver(o);
            if (this.countObservers() != i) {
                getNewPatten(o).ifPresent(pattens::remove);
            }
        }

        @Override
        public synchronized void deleteObservers() {
            super.deleteObservers();
            pattens.clear();
        }

        public List<Bytes> getPattens() {
            return Collections.unmodifiableList(pattens);
        }
    }

}
