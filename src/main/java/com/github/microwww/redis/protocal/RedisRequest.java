package com.github.microwww.redis.protocal;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.PubSub;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.jedis.JedisInputStream;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.IoRunnable;
import com.github.microwww.redis.util.NotNull;

import java.util.Arrays;

public class RedisRequest {
    private static final Logger log = LogFactory.getLogger(RedisRequest.class);

    private final ChannelContext context;
    private final String command;
    private final RequestParams[] params;
    private final RedisServer server;
    private JedisInputStream inputStream;
    private IoRunnable next = () -> {
        log.debug("Flush {} outputStream {}", this.getCommand(), this.getContext().getRemoteHost());
        this.getOutputStream().flush();
    };

    public static RedisRequest warp(RedisRequest request, String cmd, RequestParams[] params) {
        RedisRequest rq = new RedisRequest(request.getServer(), request.getContext(), cmd, params);
        rq.setInputStream(request.getInputStream());
        return rq;
    }

    public RedisRequest(RedisServer server, ChannelContext context, String command, RequestParams[] params) {
        this.server = server;
        this.context = context;
        this.command = command;
        this.params = params;
    }

    public RedisRequest(RedisServer server, ChannelContext context, RequestParams[] request) {
        this(server, context, request[0].isNotNull().getByteArray2string(), new RequestParams[request.length - 1]);
        System.arraycopy(request, 1, this.params, 0, this.params.length);
    }

    public ChannelContext getContext() {
        return context;
    }

    public String getCommand() {
        return command;
    }

    @NotNull
    public RequestParams[] getParams() {
        return Arrays.copyOf(params, params.length);
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
        int count = this.getParams().length;
        Assert.isTrue(count == expect,
                String.format("The number of arguments is not as expected, expect: %d, BUT: %d", expect, count));
    }

    public void expectArgumentsCountBigger(int expect) {
        int count = this.getParams().length;
        Assert.isTrue(count > expect,
                String.format("The number of arguments is not as expected, expect: > %d, BUT: = %d", expect, count));
    }

    /**
     * Greater than or equal to
     *
     * @param expect args count
     */
    public void expectArgumentsCountGE(int expect) {
        int count = this.getParams().length;
        Assert.isTrue(count >= expect,
                String.format("The number of arguments is not as expected, expect: >= %d, BUT: = %d", expect, count));
    }

    public void expectArgumentsCountLitter(int expect) {
        int count = this.getParams().length;
        Assert.isTrue(count < expect,
                String.format("The number of arguments is not as expected, expect: < %d, BUT: = %d", expect, count));
    }

    public void expectArgumentsCountLE(int expect) {
        int count = this.getParams().length;
        Assert.isTrue(count <= expect,
                String.format("The number of arguments is not as expected, expect: <= %d, BUT: = %d", expect, count));
    }

    public RedisServer getServer() {
        return server;
    }

    public IoRunnable getNext() {
        return next;
    }

    /**
     * set next, remember to invoke `output.flush();`
     *
     * @param next In request thread running
     */
    public void setNext(IoRunnable next) {
        this.next = next;
    }

    public RequestSession getSessions() {
        return context.getSessions();
    }
}
