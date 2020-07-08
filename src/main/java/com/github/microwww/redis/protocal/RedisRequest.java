package com.github.microwww.redis.protocal;

import com.github.microwww.redis.ChannelOutputStream;
import com.github.microwww.redis.ConsumerIO;
import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class RedisRequest {

    private final SocketChannel channel;
    private RedisInputStream inputStream;
    private final String command;
    private ExpectRedisRequest[] args;
    private RedisServer server;
    private ConsumerIO<RedisRequest> next = (r) -> {
    };

    public RedisRequest(RedisServer server, SocketChannel channel, String command, ExpectRedisRequest[] params) {
        this.server = server;
        this.channel = channel;
        this.command = command;
        this.args = params;
    }

    public RedisRequest(RedisServer server, SocketChannel channel, ExpectRedisRequest[] request) {
        this(server, channel, request[0].isNotNull().getByteArray2string(), new ExpectRedisRequest[request.length - 1]);
        System.arraycopy(request, 1, this.args, 0, this.args.length);
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public String getCommand() {
        return command;
    }

    @NotNull
    public ExpectRedisRequest[] getArgs() {
        return Arrays.copyOf(args, args.length);
    }

    public RedisOutputStream getOutputStream() {
        return new RedisOutputStream(new ChannelOutputStream(this.channel));
    }

    public RedisInputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(RedisInputStream inputStream) {
        Assert.isTrue(this.inputStream == null, "Not allowed to reset input !");
        this.inputStream = inputStream;
    }

    public RequestSession getSessions() {
        return server.getSession(channel);
    }

    public RedisDatabase getDatabase() {
        int index = this.getSessions().getDatabase();
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

    public RedisServer getServer() {
        return server;
    }

    public ConsumerIO<RedisRequest> getNext() {
        return next;
    }

    public void setNext(ConsumerIO<RedisRequest> next) {
        this.next = next;
    }
}
