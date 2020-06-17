package com.github.microwww.protocal;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ServiceProtocol {

    ServerOperation serverOperation;
    DatabaseOperation databaseOperation;

    private static final ServiceProtocol protocol = new ServiceProtocol();

    public ServiceProtocol() {
        databaseOperation = new DatabaseOperation();
        serverOperation = new ServerOperation();
    }

    public static void exec(RedisRequest request) {
        String cmd = request.getCommand();
        protocol.exec(cmd, request);
    }

    public void exec(String cmd, RedisRequest request) {
        try {
            Method method = protocol.getClass().getMethod(cmd.toLowerCase(), RedisRequest.class);
            method.invoke(protocol, request);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("Not support this command :" + cmd, e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new UnsupportedOperationException("Not allow run this command :" + cmd, e);
        }
    }

    public void ping(RedisRequest request) throws IOException {
        serverOperation.ping(request);
    }
}
