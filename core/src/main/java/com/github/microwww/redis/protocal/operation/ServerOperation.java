package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.database.RedisDatabase;
import com.github.microwww.redis.database.Schema;
import com.github.microwww.redis.exception.Run;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.RequestSession;
import com.github.microwww.redis.protocal.jedis.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;

public class ServerOperation extends AbstractOperation {

    public static final Logger log = LogFactory.getLogger(ServerOperation.class);

    //BGREWRITEAOF
    //BGSAVE
    public void bgsave(RedisRequest request) throws IOException {
        request.expectArgumentsCountLE(1);
        boolean started = request.getServer().bgsave();
        if (started) {
            request.getOutputProtocol().writer("Background saving started");
        } else {
            request.getOutputProtocol().writerError(RedisOutputProtocol.Level.ERR, "ERR Background save already in progress");
        }
    }

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
    public void lastsave(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        long lastSaveTime = request.getServer().getLastSaveTime();
        request.getOutputProtocol().writer(lastSaveTime / 1000);
    }

    //MONITOR
    //PSYNC
    //SAVE
    public void save(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        request.getServer().save();
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
    }

    //SHUTDOWN
    public void shutdown(RedisRequest request) throws IOException {
        request.expectArgumentsCountLE(1);
        boolean doSave = true;
        if (request.getParams().length > 0) {
            String opt = request.getParams()[0].getByteArray2string();
            if ("NOSAVE".equalsIgnoreCase(opt)) {
                doSave = false;
            } else if ("SAVE".equalsIgnoreCase(opt)) {
                doSave = true;
            }
        }
        if (doSave) {
            request.getServer().save();
        }
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
        request.getOutputProtocol().flush();
        request.getServer().close();
    }
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
        ID {
            @Override
            public void operation(RedisRequest request) throws IOException {
                int id = request.getSessions().ID;
                request.getOutputProtocol().writer(id);
            }
        },
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
                        RequestSession ses = client.getSessions();
                        ss.append("id=").append(ses.ID).append("\n");
                        ss.append("addr=").append(addr.getHostName()).append(":").append(addr.getPort()).append("\n");
                        ses.getInfo().forEach((k, v) -> {
                            ss.append(k + "=").append(v).append("\n");
                        });
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
        },
        SETINFO {
            @Override
            public void operation(RedisRequest request) throws IOException {
                request.expectArgumentsCount(3);
                String attr = request.getParams()[1].getByteArray2string();
                request.getSessions().setInfo(attr, request.getParams()[2].getByteArray2string());
                request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
            }
        },
        INFO {
            @Override
            public void operation(RedisRequest request) throws IOException {
                request.expectArgumentsCount(3);
                String attr = request.getParams()[1].getByteArray2string();
                request.getSessions().setInfo(attr, request.getParams()[2].getByteArray2string());
                request.getOutputProtocol().writer(Protocol.Keyword.OK.raw);
            }
        };

        public abstract void operation(RedisRequest request) throws IOException;
    }
}
