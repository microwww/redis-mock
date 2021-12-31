package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.exception.Run;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
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
        request.getOutputProtocol().writer(size);
    }

    //DEBUG OBJECT
    //DEBUG SEGFAULT
    //FLUSHALL
    public void flushall(RedisRequest request) throws IOException {
        request.expectArgumentsCountLE(1);// ASYNC|SYNC
        Schema db = request.getServer().getSchema();
        db.clearDatabase();
        request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
    }

    //FLUSHDB
    public void flushdb(RedisRequest request) throws IOException {
        request.expectArgumentsCountLE(1);// ASYNC|SYNC
        RedisDatabase db = request.getDatabase();
        db.clear();
        request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
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
        request.getOutputProtocol().writerMulti((seconds + "").getBytes(), (micro + "000").getBytes());
    }

    public enum Client {
        GETNAME {
            @Override
            public void operation(RedisRequest request) throws IOException {
                Optional<String> name = request.getSessions().getName();
                if (name.isPresent()) {
                    request.getOutputProtocol().writer(name.get());
                } else {
                    request.getOutputProtocol().writerNull();
                }
            }
        },
        KILL {
            @Override
            public void operation(RedisRequest request) throws IOException {
                request.expectArgumentsCount(2);
                request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
                request.setNext(() -> {
                    request.getOutputProtocol().flush();
                    ChannelContext context = request.getContext();
                    Run.ignoreException(log, context::closeChannel);
                });
            }
        },
        LIST {
            @Override
            public void operation(RedisRequest request) throws IOException {
                Set<ChannelContext> clients = request.getServer().getSockets().getClients();
                StringBuilder ss = new StringBuilder();
                for (ChannelContext client : clients) {
                    try {
                        InetSocketAddress addr = client.getRemoteAddress();
                        ss.append("addr=").append(addr.getHostName()).append(":").append(addr.getPort()).append("\n");
                    } catch (Exception ex) {// ignore
                        log.debug("List client error", ex);
                    }
                }
                request.getOutputProtocol().writer(ss.toString());
            }
        },
        SETNAME {
            @Override
            public void operation(RedisRequest request) throws IOException {
                request.expectArgumentsCount(2);
                String name = request.getParams()[1].getByteArray2string();
                request.getSessions().setName(name);
                request.getOutputProtocol().writer(name);
            }
        };

        public abstract void operation(RedisRequest request) throws IOException;
    }
}
