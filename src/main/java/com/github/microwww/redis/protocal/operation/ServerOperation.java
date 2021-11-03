package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.exception.Run;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;

public class ServerOperation extends AbstractOperation {

    public static final Logger log = LogFactory.getLogger(ServerOperation.class);

    //BGREWRITEAOF
    //BGSAVE
    //CLIENT GETNAME
    public void client(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        String cmd = request.getParams()[0].getByteArray2string();
        Client.valueOf(cmd.toUpperCase()).operation(request);
    }

    //CLIENT KILL
    //CLIENT LIST
    //CLIENT SETNAME
    //CONFIG GET
    //CONFIG RESETSTAT
    //CONFIG REWRITE
    //CONFIG SET
    //DBSIZE
    public void dbsize(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        int index = request.getSessions().getDatabase();
        int size = request.getServer().getSchema().getRedisDatabases(index).getMapSize();
        RedisOutputProtocol.writer(request.getOutputStream(), size);
    }

    //DEBUG OBJECT
    //DEBUG SEGFAULT
    //FLUSHALL
    public void flushall(RedisRequest request) throws IOException {
        request.expectArgumentsCountLE(1);// ASYNC|SYNC
        Schema db = request.getServer().getSchema();
        db.clearDatabase();
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
    }

    //FLUSHDB
    public void flushdb(RedisRequest request) throws IOException {
        request.expectArgumentsCountLE(1);// ASYNC|SYNC
        RedisDatabase db = request.getDatabase();
        db.clear();
        RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
    }

    //INFO
    //LASTSAVE
    //MONITOR
    //PSYNC
    //SAVE
    //SHUTDOWN
    //SLAVEOF
    //SLOWLOG
    //SYNC
    //TIME
    public void time(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        long time = System.currentTimeMillis();
        long seconds = time / 1_000;
        long micro = time % 1_000;
        RedisOutputProtocol.writerMulti(request.getOutputStream(), (seconds + "").getBytes(), (micro + "000").getBytes());
    }

    public void kill(RedisRequest request) {
        request.expectArgumentsCount(1);
        String address = request.getParams()[0].getByteArray2string();
        ChannelContext context = request.getContext();
        Run.ignoreException(log, context::closeChannel);
        request.setNext(e -> log.info("USER KILL: " + address));
    }

    public enum Client {
        GETNAME {
            @Override
            public void operation(RedisRequest request) throws IOException {
                Optional<String> name = request.getSessions().getName();
                if (name.isPresent()) {
                    RedisOutputProtocol.writer(request.getOutputStream(), name.get());
                } else {
                    RedisOutputProtocol.writerNull(request.getOutputStream());
                }
            }
        },
        KILL {
            @Override
            public void operation(RedisRequest request) throws IOException {
                request.expectArgumentsCount(2);
                String addr = request.getParams()[1].getByteArray2string();
                RequestParams[] err = {new RequestParams(addr.getBytes())};
                RedisRequest rqu = RedisRequest.warp(request, "KILL", err);
                RedisOutputProtocol.writer(request.getOutputStream(), Protocol.Keyword.OK.raw);
                request.setNext(e -> {
                    request.getOutputStream().flush();
                    request.getServer().getSchema().submit(rqu);// new thread
                });
            }
        },
        LIST {
            @Override
            public void operation(RedisRequest request) throws IOException {
                Set<ChannelContext> clients = request.getServer().getSockets().getClients();
                StringBuilder ss = new StringBuilder();
                for (ChannelContext client : clients) {
                    InetSocketAddress addr = client.getRemoteAddress();
                    ss.append("addr=").append(addr.getHostName()).append(":").append(addr.getPort()).append("\n");
                }
                RedisOutputProtocol.writer(request.getOutputStream(), ss.toString());
            }
        },
        SETNAME {
            @Override
            public void operation(RedisRequest request) throws IOException {
                request.expectArgumentsCount(2);
                String name = request.getParams()[1].getByteArray2string();
                request.getSessions().setName(name);
                RedisOutputProtocol.writer(request.getOutputStream(), name);
            }
        };

        public abstract void operation(RedisRequest request) throws IOException;
    }
}
