package com.github.microwww.redis.protocal;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.ConsumerIO;
import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.database.PubSub;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.jedis.JedisInputStream;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;

import java.util.Arrays;

public class RedisRequest {
    private static final Logger log = LogFactory.getLogger(RedisRequest.class);

    private final ChannelContext context;
    private final String command;
    private final ExpectRedisRequest[] args;
    private final RedisServer server;
    private JedisInputStream inputStream;
    private ConsumerIO<Object> next = (r) -> {
        log.debug("Flush outputStream");
        this.getOutputStream().flush();
    };

    public static RedisRequest warp(RedisRequest request, ExpectRedisRequest[] requests) {
        RedisRequest rq = new RedisRequest(request.getServer(), request.getContext(), requests);
        rq.setInputStream(request.getInputStream());
        return rq;
    }

    public static RedisRequest warp(RedisRequest request, String cmd, ExpectRedisRequest[] params) {
        RedisRequest rq = new RedisRequest(request.getServer(), request.getContext(), cmd, params);
        rq.setInputStream(request.getInputStream());
        return rq;
    }

    public RedisRequest(RedisServer server, ChannelContext context, String command, ExpectRedisRequest[] params) {
        this.server = server;
        this.context = context;
        this.command = command;
        this.args = params;
    }

    public RedisRequest(RedisServer server, ChannelContext context, ExpectRedisRequest[] request) {
        this(server, context, request[0].isNotNull().getByteArray2string(), new ExpectRedisRequest[request.length - 1]);
        System.arraycopy(request, 1, this.args, 0, this.args.length);
    }

    public ChannelContext getContext() {
        return context;
    }

    public String getCommand() {
        return command;
    }

    @NotNull
    public ExpectRedisRequest[] getArgs() {
        return Arrays.copyOf(args, args.length);
    }

    public JedisOutputStream getOutputStream() {
        return this.context.getOutputStream();
    }

    public JedisInputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(JedisInputStream inputStream) {
        Assert.isTrue(this.inputStream == null, "Not allowed to reset input !");
        this.inputStream = inputStream;
    }

    public RedisDatabase getDatabase() {
        int index = context.getSessions().getDatabase();
        return server.getSchema().getRedisDatabases(index);
    }

    public PubSub getPubSub() {
        return server.getSchema().getPubSub();
    }

    public void expectArgumentsCount(int expect) {
        int count = this.getArgs().length;
        Assert.isTrue(count == expect,
                String.format("The number of arguments is not as expected, expect: %d, BUT: %d", expect, count));
    }

    public void expectArgumentsCountBigger(int expect) {
        int count = this.getArgs().length;
        Assert.isTrue(count > expect,
                String.format("The number of arguments is not as expected, expect: > %d, BUT: = %d", expect, count));
    }

    /**
     * Greater than or equal to
     *
     * @param expect args count
     */
    public void expectArgumentsCountGE(int expect) {
        int count = this.getArgs().length;
        Assert.isTrue(count >= expect,
                String.format("The number of arguments is not as expected, expect: >= %d, BUT: = %d", expect, count));
    }

    public void expectArgumentsCountLitter(int expect) {
        int count = this.getArgs().length;
        Assert.isTrue(count < expect,
                String.format("The number of arguments is not as expected, expect: < %d, BUT: = %d", expect, count));
    }

    public void expectArgumentsCountLE(int expect) {
        int count = this.getArgs().length;
        Assert.isTrue(count <= expect,
                String.format("The number of arguments is not as expected, expect: <= %d, BUT: = %d", expect, count));
    }

    public RedisServer getServer() {
        return server;
    }

    public ConsumerIO<Object> getNext() {
        return next;
    }

    /**
     * set next, remember to invoke `output.flush();`
     *
     * @param next In request thread running
     */
    public void setNext(ConsumerIO<Object> next) {
        this.next = next;
    }

    public RequestSession getSessions() {
        return context.getSessions();
    }
}
