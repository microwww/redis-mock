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
import java.util.stream.Stream;

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
            Bytes patten = arg.toBytes();
            NewChannelListener notify = new NewChannelListener(request.getContext(), patten, pubSub);
            try {
                notify.subscribe();
            } catch (Exception e) {
                log.warn("subscribe [{}] error", notify.patten, e);
                notify.unsubscribe();
            }
            sub[1] = patten.getBytes();
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
    public void pubsub(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        String subcommand = args[0].getByteArray2string().toLowerCase();
        switch (subcommand) {
            case "channels":
                this.subChannels(request);
                break;
            case "numsub":
                this.subNumSub(request);
                break;
            case "numpat":
                this.subNumPat(request);
                break;
            default:
                throw new UnsupportedOperationException(subcommand);
        }
    }

    private void subChannels(RedisRequest request) throws IOException {
        PubSub pubSub = request.getPubSub();
        Stream<PubSub.MessageChannel> stream = pubSub.getChannels().values().stream().filter(PubSub.MessageChannel::isActive);
        if (request.getArgs().length > 1) {
            String patten = request.getArgs()[1].getByteArray2string();
            stream = stream.filter(e -> StringUtil.antPatternMatches(patten, SafeEncoder.encode(e.getChannel().getBytes())));
        }
        Object[] objects = stream.map(PubSub.MessageChannel::getChannel).toArray(Object[]::new);
        RedisOutputProtocol.writerComplex(request.getOutputStream(), objects);
    }

    private void subNumSub(RedisRequest request) throws IOException {
        PubSub pubSub = request.getPubSub();
        int len = request.getArgs().length;
        List<Object> list = new ArrayList<>();
        if (len > 1) {
            for (int i = 1; i < len; i++) {
                Bytes bytes = request.getArgs()[i].toBytes();
                list.add(bytes);
                PubSub.MessageChannel mc = pubSub.getChannels().get(bytes);
                if (mc == null) {
                    list.add(0);
                } else {
                    list.add(mc.getNumsub());
                }
            }
        }
        // 不指定 channel 则返回空的列表
        RedisOutputProtocol.writerComplex(request.getOutputStream(), list.toArray());
    }

    // TODO :: 这个有出入, 不同的客户端对相同的 patten 是否认为是唯一的 ???
    private void subNumPat(RedisRequest request) throws IOException {
        PubSub pubSub = request.getPubSub();
        // RedisOutputProtocol.writer(request.getOutputStream(), pubSub.newChannelNotify.countObservers());
        RedisOutputProtocol.writer(request.getOutputStream(), new HashSet<>(pubSub.newChannelNotify.getPattens()).size());
    }

    //PUNSUBSCRIBE
    public void punsubscribe(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Object[] uns = new Object[3];
        uns[0] = SafeEncoder.encode("punsubscribe");
        if (args.length == 0) {
            Map<Bytes, Observer> mpa = request.getContext().getPattenSubscribe().subscribeChannels();
            Iterator<Bytes> iterator = new HashSet<>(mpa.keySet()).iterator();
            while (iterator.hasNext()) {
                Bytes next = iterator.next();
                uns[1] = next;
                NewChannelListener.find(request.getContext(), next).ifPresent(NewChannelListener::unsubscribe);
                uns[2] = request.getContext().getPattenSubscribe().subscribeChannels().size();
                RedisOutputProtocol.writerComplex(request.getOutputStream(), uns);
            }
        } else {
            for (ExpectRedisRequest arg : args) {
                Bytes bytes = arg.toBytes();
                uns[1] = bytes;
                ChannelMessageListener.find(request.getContext(), bytes).ifPresent(ChannelMessageListener::unsubscribe);
                uns[2] = request.getContext().getSubscribe().subscribeChannels().size();
                RedisOutputProtocol.writerComplex(request.getOutputStream(), uns);
            }
        }
        request.getOutputStream().flush();
    }

    //SUBSCRIBE
    public void subscribe(RedisRequest request) throws IOException {
        PubSub pubSub = request.getPubSub();
        request.expectArgumentsCountGE(1);
        ExpectRedisRequest[] args = request.getArgs();
        Object[] sub = new Object[3];
        sub[0] = "subscribe".getBytes(StandardCharsets.UTF_8);
        for (ExpectRedisRequest arg : args) {
            Bytes bytes = arg.toBytes();
            ChannelMessageListener channelMessageListener = new ChannelMessageListener(request.getContext(), bytes, pubSub);
            try {
                channelMessageListener.subscribe();
            } catch (Exception e) {
                log.warn("subscribe [{}] error", channelMessageListener.channel, e);
                channelMessageListener.unsubscribe();
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
                ChannelMessageListener.find(request.getContext(), next).ifPresent(ChannelMessageListener::unsubscribe);
                uns[2] = request.getContext().getSubscribe().subscribeChannels().size();
                RedisOutputProtocol.writerComplex(request.getOutputStream(), uns);
            }
        } else {
            for (ExpectRedisRequest arg : args) {
                Bytes bytes = arg.toBytes();
                uns[1] = bytes;
                ChannelMessageListener.find(request.getContext(), bytes).ifPresent(ChannelMessageListener::unsubscribe);
                uns[2] = request.getContext().getSubscribe().subscribeChannels().size();
                RedisOutputProtocol.writerComplex(request.getOutputStream(), uns);
            }
        }
        request.getOutputStream().flush();
    }

    /**
     * 单新建一个`消息通道`的时候会触发 update 方法, 如果匹配 patten, 则追加一个消息的监听
     */
    public static class NewChannelListener implements Observer {
        private final ChannelContext context;
        private final Bytes bytes;
        private final Pattern patten;
        private final PubSub pubSub;
        private final ChannelContext.CloseListener channelClose;
        private final Map<Bytes, ChannelMessageListener> notifies = new HashMap<>();

        public NewChannelListener(ChannelContext context, Bytes patten, PubSub pubSub) {
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
            String encode = SafeEncoder.encode(channel.getBytes());
            if (patten.matcher(encode).matches()) { // 新的channel 是否配置 patten, 如果匹配则添加一个`通道的消息`的监听
                notifies.computeIfAbsent(channel, k -> {
                    ChannelMessageListener channelMessageListener = new ChannelMessageListener(context, (Bytes) arg, pubSub);
                    channelMessageListener.setPatten(this.bytes);
                    pubSub.subscribe(channel, channelMessageListener);
                    return channelMessageListener;
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
            find(this.context, bytes).ifPresent(NewChannelListener::unsubscribe); // 删除已经存在的
            pubSub.newChannelNotify.subscribe(this);
            this.context.getPattenSubscribe().addSubscribe(bytes, this);
        }

        public static Optional<NewChannelListener> find(ChannelContext context, Bytes bytes) {
            return context.getPattenSubscribe().getSubscribe(bytes);
        }

        public Bytes getPatten() {
            return bytes;
        }
    }

    /**
     * publish 的消息的监听器
     */
    public static class ChannelMessageListener implements Observer {
        private final ChannelContext context;
        private Bytes patten; // update 方法, 如果不为null 标识这是一个 `psubscribe`, 否则是 `subscribe`
        private final Bytes channel;
        private final PubSub pubSub;
        private final ChannelContext.CloseListener channelClose;

        public ChannelMessageListener(ChannelContext context, Bytes channel, PubSub pubSub) {
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
                List<Object> msg = new ArrayList<>(4);
                if (this.getPatten().isPresent()) {
                    msg.add(SafeEncoder.encode("pmessage"));
                    msg.add(this.getPatten().get().getBytes());
                } else {
                    msg.add(SafeEncoder.encode("message"));
                }
                msg.add(channel);
                Assert.isTrue(arg instanceof Bytes, "Observable publish must be `Bytes`");
                msg.add(arg);
                RedisOutputProtocol.writerComplex(context.getOutputStream(), msg.toArray());
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
            find(this.context, channel).ifPresent(ChannelMessageListener::unsubscribe); // 删除已经存在的
            this.context.getSubscribe().addSubscribe(channel, this);
            pubSub.subscribe(channel, this);
        }

        public static Optional<ChannelMessageListener> find(ChannelContext context, Bytes bytes) {
            return context.getSubscribe().getSubscribe(bytes);
        }

        public ChannelMessageListener setPatten(Bytes patten) {
            this.patten = patten;
            return this;
        }

        public Optional<Bytes> getPatten() {
            return Optional.ofNullable(patten);
        }
    }
}
