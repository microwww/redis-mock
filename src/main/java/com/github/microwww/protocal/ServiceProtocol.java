package com.github.microwww.protocal;

import com.github.microwww.protocal.operation.StringOperation;
import com.github.microwww.protocal.operation.ConnectionOperation;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ServiceProtocol {

    ConnectionOperation serverOperation;
    StringOperation databaseOperation;

    private static final ServiceProtocol protocol = new ServiceProtocol();

    public ServiceProtocol() {
        databaseOperation = new StringOperation();
        serverOperation = new ConnectionOperation();
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
            throw new UnsupportedOperationException("Not support this command : " + cmd, e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new UnsupportedOperationException("Not allow run this command : " + cmd, e);
        }
    }

    public void ping(RedisRequest request) throws IOException {
        serverOperation.ping(request);
    }

    public void select(RedisRequest request) throws IOException {
        databaseOperation.select(request);
    }

    public void set(RedisRequest request) throws IOException {
        databaseOperation.set(request);
    }

    public void expire(RedisRequest request) throws IOException {
        databaseOperation.expire(request);
    }

    public void get(RedisRequest request) throws IOException {
        databaseOperation.get(request);
    }

}
