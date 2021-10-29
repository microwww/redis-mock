package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.database.PubSub;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.SafeEncoder;
import com.github.microwww.redis.util.StringUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public class PubSubOperation extends AbstractOperation {
    private static final Logger log = LogFactory.getLogger(PubSubOperation.class);

    //PSUBSCRIBE
    public void psubscribe(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        Object[] sub = new Object[3];
        sub[0] = "psubscribe".getBytes(StandardCharsets.UTF_8);
        PubSub pubSub = request.getPubSub();
        for (ExpectRedisRequest arg : args) {
            Bytes bytes = arg.toBytes();
            PattenNotify notify = new PattenNotify(request.getContext(), bytes, pubSub);
            try {
                notify.subscribe();
            } catch (Exception e) {
                log.warn("subscribe [{}] error", notify.patten, e);
                notify.unsubscribe();
            }
            sub[1] = bytes.getBytes();
            sub[2] = request.getContext().getPattenSubscribe().subscribeChannels().size();
            RedisOutputProtocol.writerComplex(request.getOutputStream(), sub);
        }
    }

    //PUBLISH
    public void publish(RedisRequest request) throws IOException {
        PubSub pubSub = request.getPubSub();
        request.expectArgumentsCount(2);
        ExpectRedisRequest[] args = request.getArgs();
        Bytes channel = args[0].toBytes();
        Bytes message = args[1].toBytes();
        int count = pubSub.publish(channel, message);
        RedisOutputProtocol.writer(request.getOutputStream(), count);
    }

    //PUBSUB
    //PUNSUBSCRIBE
    //SUBSCRIBE
    public void subscribe(RedisRequest request) throws IOException {
        PubSub pubSub = request.getPubSub();
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        Object[] sub = new Object[3];
        sub[0] = "subscribe".getBytes(StandardCharsets.UTF_8);
        for (ExpectRedisRequest arg : args) {
            Bytes bytes = arg.toBytes();
            Notify notify = new Notify(request.getContext(), bytes, pubSub);
            try {
                notify.subscribe();
            } catch (Exception e) {
                log.warn("subscribe [{}] error", notify.channel, e);
                notify.unsubscribe();
            }
            sub[1] = bytes.getBytes();
            sub[2] = request.getContext().getSubscribe().subscribeChannels().size();
            RedisOutputProtocol.writerComplex(request.getOutputStream(), sub);
        }
    }

    //UNSUBSCRIBE
    public void unsubscribe(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Object[] uns = new Object[3];
        uns[0] = "unsubscribe".getBytes(StandardCharsets.UTF_8);
        if (args.length == 0) {
            Map<Bytes, Observer> mpa = request.getContext().getSubscribe().subscribeChannels();
            Iterator<Bytes> iterator = new HashSet<>(mpa.keySet()).iterator();
            while (iterator.hasNext()) {
                Bytes next = iterator.next();
                uns[1] = next;
                Notify.find(request.getContext(), next).ifPresent(Notify::unsubscribe);
                uns[2] = request.getContext().getSubscribe().subscribeChannels().size();
                RedisOutputProtocol.writerComplex(request.getOutputStream(), uns);
            }
        } else {
            for (ExpectRedisRequest arg : args) {
                Bytes bytes = arg.toBytes();
                uns[1] = bytes;
                Notify.find(request.getContext(), bytes).ifPresent(Notify::unsubscribe);
                uns[2] = request.getContext().getSubscribe().subscribeChannels().size();
                RedisOutputProtocol.writerComplex(request.getOutputStream(), uns);
            }
        }
        request.getOutputStream().flush();
    }

    public static class PattenNotify implements Observer {
        private final ChannelContext context;
        private final Bytes bytes;
        private final Pattern patten;
        private final PubSub pubSub;
        private final ChannelContext.CloseListener channelClose;
        private final Map<Bytes, Notify> notifies = new HashMap<>();

        public PattenNotify(ChannelContext context, Bytes patten, PubSub pubSub) {
            this.context = context;
            this.bytes = patten;
            this.patten = StringUtil.antPattern(SafeEncoder.encode(patten.getBytes()));
            this.pubSub = pubSub;
            this.channelClose = context.addCloseListener(this::close);
        }

        private void close(ChannelContext context) {
            try {// channel close to unsubscribe !
                unsubscribe();
            } catch (Exception e) {
            }
        }

        @Override
        public void update(Observable o, Object arg) {
            Bytes channel = (Bytes) arg;
            if (patten.matcher(SafeEncoder.encode(channel.getBytes())).matches()) {
                notifies.computeIfAbsent(channel, k -> {
                    Notify notify = new Notify(context, (Bytes) arg, pubSub);
                    pubSub.subscribe(channel, notify);
                    return notify;
                });

            }
        }

        public void unsubscribe() {
            pubSub.newChannelNotify.unsubscribe(this);
            notifies.values().forEach(e -> {
                pubSub.unsubscribe(e.channel, e);
            });
            context.getPattenSubscribe().removeSubscribe(bytes);
            context.removeCloseListener(channelClose);
        }

        public void subscribe() {
            find(this.context, bytes).ifPresent(PattenNotify::unsubscribe); // 删除已经存在的
            pubSub.newChannelNotify.subscribe(this);
            this.context.getPattenSubscribe().addSubscribe(bytes, this);
        }

        public static Optional<PattenNotify> find(ChannelContext context, Bytes bytes) {
            return context.getPattenSubscribe().getSubscribe(bytes);
        }
    }

    public static class Notify implements Observer {
        private final ChannelContext context;
        private final Bytes channel;
        private final PubSub pubSub;
        private final ChannelContext.CloseListener channelClose;

        public Notify(ChannelContext context, Bytes channel, PubSub pubSub) {
            this.context = context;
            this.channel = channel;
            this.pubSub = pubSub;
            this.channelClose = context.addCloseListener(this::close);
        }

        private void close(ChannelContext context) {
            try {// channel close to unsubscribe !
                unsubscribe();
            } catch (Exception e) {
            }
        }

        @Override
        public void update(Observable o, Object arg) {
            try {
                Object[] msg = new Object[3];
                msg[0] = "message".getBytes(StandardCharsets.UTF_8);
                msg[1] = channel;
                Assert.isTrue(arg instanceof Bytes, "Observable publish must be `Bytes`");
                msg[2] = arg;
                RedisOutputProtocol.writerComplex(context.getOutputStream(), msg);
                context.getOutputStream().flush();
            } catch (Exception e) {
                log.warn("Notify subscriber error, ignore, {}", e);
            }
        }

        public void unsubscribe() {
            pubSub.unsubscribe(channel, this);
            context.getSubscribe().removeSubscribe(channel);
            context.removeCloseListener(channelClose);
        }

        public void subscribe() {
            find(this.context, channel).ifPresent(Notify::unsubscribe); // 删除已经存在的
            this.context.getSubscribe().addSubscribe(channel, this);
            pubSub.subscribe(channel, this);
        }

        public static Optional<Notify> find(ChannelContext context, Bytes bytes) {
            return context.getSubscribe().getSubscribe(bytes);
        }
    }
}
