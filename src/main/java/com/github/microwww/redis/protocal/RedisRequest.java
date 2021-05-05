package com.github.microwww.redis.protocal;

import com.github.microwww.redis.ChannelOutputStream;
import com.github.microwww.redis.ConsumerIO;
import com.github.microwww.redis.ExpectRedisRequest;
import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.protocal.jedis.JedisInputStream;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.util.Assert;
import com.github.microwww.redis.util.NotNull;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class RedisRequest {

    private final SocketChannel channel;
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

    public RedisRequest(RedisServer server, SocketChannel channel, String command, ExpectRedisRequest[] params) {
        this.server = server;
        this.channel = channel;
        this.command = command;
        this.args = params;
        this.outputStream = new JedisOutputStream(new ChannelOutputStream(this.channel));
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

    public RequestSession getSessions() throws IOException {
        return server.getSession(channel);
    }

    public RedisDatabase getDatabase() {
        try {
            int index = this.getSessions().getDatabase();
            return server.getSchema().getRedisDatabases(index);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
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
}
