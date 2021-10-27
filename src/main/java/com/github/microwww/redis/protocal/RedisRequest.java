package com.github.microwww.redis.protocal;

import com.github.microwww.redis.*;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.protocal.jedis.JedisInputStream;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;

import java.io.IOException;
import java.util.Arrays;

public class RedisRequest {

    private final ChannelContext channel;
    private final String command;
    private final ExpectRedisRequest[] args;
    private final RedisServer server;
    private JedisOutputStream outputStream;
    private JedisInputStream inputStream;
    private ConsumerIO<Object> next = (r) -> {
        this.getOutputStream().flush();
    };

    public static RedisRequest warp(RedisRequest request, ExpectRedisRequest[] requests) {
        RedisRequest rq = new RedisRequest(request.getServer(), request.getChannel(), requests);
        rq.setOutputStream(request.getOutputStream());
        rq.setInputStream(request.getInputStream());
        return rq;
    }

    public static RedisRequest warp(RedisRequest request, String cmd, ExpectRedisRequest[] params) {
        RedisRequest rq = new RedisRequest(request.getServer(), request.getChannel(), cmd, params);
        rq.setOutputStream(request.getOutputStream());
        rq.setInputStream(request.getInputStream());
        return rq;
    }

    public RedisRequest(RedisServer server, ChannelContext channel, String command, ExpectRedisRequest[] params) {
        this.server = server;
        this.channel = channel;
        this.command = command;
        this.args = params;
        this.outputStream = new JedisOutputStream(new ChannelOutputStream(this.channel.getChannel()));
    }

    public RedisRequest(RedisServer server, ChannelContext channel, ExpectRedisRequest[] request) {
        this(server, channel, request[0].isNotNull().getByteArray2string(), new ExpectRedisRequest[request.length - 1]);
        System.arraycopy(request, 1, this.args, 0, this.args.length);
    }

    public ChannelContext getChannel() {
        return channel;
    }

    public String getCommand() {
        return command;
    }

    @NotNull
    public ExpectRedisRequest[] getArgs() {
        return Arrays.copyOf(args, args.length);
    }

    public JedisOutputStream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(JedisOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public JedisInputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(JedisInputStream inputStream) {
        Assert.isTrue(this.inputStream == null, "Not allowed to reset input !");
        this.inputStream = inputStream;
    }

    public RedisDatabase getDatabase() {
        int index = channel.getSessions().getDatabase();
        return server.getSchema().getRedisDatabases(index);
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
        return channel.getSessions();
    }
}
