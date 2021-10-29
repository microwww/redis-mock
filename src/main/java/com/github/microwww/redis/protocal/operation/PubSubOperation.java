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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PubSubOperation extends AbstractOperation {
    private static final Logger log = LogFactory.getLogger(PubSubOperation.class);

    //PSUBSCRIBE
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
            Notify notify = new Notify(request.getChannel(), bytes, pubSub);
            try {
                notify.subscribe();
            } catch (Exception e) {
                log.warn("subscribe [{}] error", notify.bytes, e);
                notify.unsubscribe();
            }
            sub[1] = bytes.getBytes();
            sub[2] = request.getChannel().subscribeChannels().size();
            RedisOutputProtocol.writerComplex(request.getOutputStream(), sub);
        }
    }

    //UNSUBSCRIBE
    public void unsubscribe(RedisRequest request) throws IOException {
        ExpectRedisRequest[] args = request.getArgs();
        Object[] uns = new Object[3];
        uns[0] = "unsubscribe".getBytes(StandardCharsets.UTF_8);
        if (args.length == 0) {
            Map<Bytes, Observer> mpa = request.getChannel().subscribeChannels();
            Iterator<Bytes> iterator = new HashSet<>(mpa.keySet()).iterator();
            while (iterator.hasNext()) {
                Bytes next = iterator.next();
                uns[1] = next;
                Notify.find(request.getChannel(), next).ifPresent(Notify::unsubscribe);
                uns[2] = request.getChannel().subscribeChannels().size();
                RedisOutputProtocol.writerComplex(request.getOutputStream(), uns);
            }
        } else {
            for (ExpectRedisRequest arg : args) {
                Bytes bytes = arg.toBytes();
                uns[1] = bytes;
                Notify.find(request.getChannel(), bytes).ifPresent(Notify::unsubscribe);
                uns[2] = request.getChannel().subscribeChannels().size();
                RedisOutputProtocol.writerComplex(request.getOutputStream(), uns);
            }
        }
        request.getOutputStream().flush();
    }

    public static class Notify implements Observer {
        private final ChannelContext context;
        private final Bytes bytes;
        private final PubSub pubSub;
        private final ChannelContext.CloseListener channelClose;

        public Notify(ChannelContext context, Bytes bytes, PubSub pubSub) {
            this.context = context;
            this.bytes = bytes;
            this.pubSub = pubSub;
            this.channelClose = context.addCloseListener(this::_unsubscribe);
        }

        private void _unsubscribe(ChannelContext context) {
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
                msg[1] = bytes;
                Assert.isTrue(arg instanceof Bytes, "Observable publish must be `Bytes`");
                msg[2] = arg;
                RedisOutputProtocol.writerComplex(context.getOutputStream(), msg);
                context.getOutputStream().flush();
            } catch (Exception e) {
                log.warn("Notify subscriber error, ignore, {}", e);
            }
        }

        public void unsubscribe() {
            pubSub.unsubscribe(bytes, this);
            context.removeSubscribe(bytes);
            context.removeCloseListener(channelClose);
        }

        public void subscribe() {
            Notify.find(this.context, bytes).ifPresent(Notify::unsubscribe);
            this.context.addSubscribe(bytes, this);
            pubSub.subscribe(bytes, this);
        }

        public static Optional<Notify> find(ChannelContext context, Bytes bytes) {
            return context.getSubscribe(bytes);
        }
    }
}
