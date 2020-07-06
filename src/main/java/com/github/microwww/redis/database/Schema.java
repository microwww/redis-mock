package com.github.microwww.redis.database;

import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisArgumentsException;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.operation.*;
import com.github.microwww.redis.util.Assert;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Schema {
    private static final Logger log = LogFactory.getLogger(Schema.class);

    public static final int DEFAULT_SCHEMA_SIZE = 16;
    private static AbstractOperation[] SUPPORT_OPERATION = new AbstractOperation[]{
            new ConnectionOperation(),
            new HashOperation(),
            new KeyOperation(),
            new ListOperation(),
            new PubSubOperation(),
            new ScriptOperation(),
            new ServerOperation(),
            new SetOperation(),
            new SortedSetOperation(),
            new StringOperation(),
            new TransactionOperation()
    };

    private final int size;
    private final RedisDatabase[] redisDatabases;
    private final List<AbstractOperation> operations;
    private final Map<String, Invoker> invokers = new ConcurrentHashMap<>();

    public Schema(int size, AbstractOperation... operations) {
        Assert.isTrue(size > 0, "Database SIZE > 0");
        this.size = size;
        this.redisDatabases = new RedisDatabase[size];
        this.operations = ops(operations);
        init();
    }


    private List<AbstractOperation> ops(AbstractOperation[] operations) {
        List<AbstractOperation> list = new ArrayList<>();
        list.addAll(Arrays.asList(operations));
        list.addAll(Arrays.asList(SUPPORT_OPERATION));
        return Collections.unmodifiableList(list);
    }

    private void init() {
        for (int i = 0; i < this.redisDatabases.length; i++) {
            redisDatabases[i] = new RedisDatabase();
        }
    }

    public RedisDatabase getRedisDatabases(int i) {
        return redisDatabases[i];
    }

    public int getSize() {
        return size;
    }

    public List<AbstractOperation> getOperations() {
        return operations;
    }

    public void exec(RedisRequest request) throws IOException {
        String cmd = request.getCommand();
        try {
            this.exec(cmd, request);
        } catch (RedisArgumentsException error) {
            RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, error.getMessage());
        } catch (RuntimeException e) {
            String message = e.getMessage();
            log.error("Server error ! {}", message, e);
            String msg = message;
            if (msg != null) {
                msg = message.replaceAll("\\r", "");
            }
            RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR,
                    String.format("Server run error ! : %s, %s", e.getClass().getName(), msg));
        }
    }

    public void exec(String cmd, RedisRequest request) throws IOException {
        Invoker invoker = invokers.get(cmd);
        if (invoker == null) {
            tryInvoke(cmd);
            invoker = invokers.get(cmd);
        }
        invoker.invoke(request);
    }

    public synchronized void tryInvoke(String cmd) {
        if (invokers.get(cmd) != null) {
            return;
        }
        for (AbstractOperation protocol : operations) {
            try {
                Method method = protocol.getClass().getMethod(cmd.toLowerCase(), RedisRequest.class);
                invokers.put(cmd, new Invoker(protocol, method));
                return;
            } catch (NoSuchMethodException e) {
                //throw new UnsupportedOperationException("Not support this command : " + cmd, e);
            }
        }
        try {
            Method method = this.getClass().getMethod("unsupportedOperation", RedisRequest.class);
            invokers.put(cmd, new Invoker(this, method));
        } catch (NoSuchMethodException e) {//
            Assert.notHere("This class must has unsupportedOperation method", e);
        }
    }

    public void unsupportedOperation(RedisRequest request) throws IOException {
        RedisOutputProtocol.writerError(request.getOutputStream(), RedisOutputProtocol.Level.ERR, "unsupported operation now");
    }

    public synchronized void clearDatabase() {
        for (RedisDatabase db : this.redisDatabases) {
            db.clear();
        }
    }

    public static class Invoker {
        public final Object instance;
        public final Method method;

        public Invoker(Object instance, Method method) {
            this.instance = instance;
            this.method = method;
        }

        public void invoke(Object... args) throws IOException {
            try {
                method.invoke(instance, args);
            } catch (IllegalAccessException | InvocationTargetException e) {
                if (e.getCause() != null) {
                    if (e.getCause() instanceof IOException) {
                        throw (IOException) e.getCause();
                    } else if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    }
                }
                throw new RuntimeException(e);
            }
        }
    }
}
