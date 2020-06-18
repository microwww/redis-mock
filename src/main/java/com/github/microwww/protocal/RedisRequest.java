package com.github.microwww.protocal;

import com.github.microwww.ChannelOutputStream;
import com.github.microwww.ExpectRedisRequest;
import com.github.microwww.RedisServer;
import com.github.microwww.database.RedisDatabase;
import com.github.microwww.util.Assert;
import com.github.microwww.util.NotNull;
import redis.clients.util.RedisOutputStream;

import java.nio.channels.SocketChannel;

public class RedisRequest {

    private final SocketChannel channel;
    private final String command;
    private final ExpectRedisRequest[] args;
    private RedisServer server;

    public RedisRequest(RedisServer server, SocketChannel channel, ExpectRedisRequest[] req) {
        this.server = server;
        this.channel = channel;
        this.command = new String(req[0].isNotNull().getByteArray()); // 命令
        this.args = new ExpectRedisRequest[req.length - 1];
        System.arraycopy(req, 1, args, 0, args.length);
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public String getCommand() {
        return command;
    }

    @NotNull
    public ExpectRedisRequest[] getArgs() {
        return args;
    }

    public RedisOutputStream getOutputStream() {
        return new RedisOutputStream(new ChannelOutputStream(this.channel));
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

    public RedisServer getServer() {
        return server;
    }
}
