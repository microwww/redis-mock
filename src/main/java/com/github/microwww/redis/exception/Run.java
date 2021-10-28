package com.github.microwww.redis.exception;

import com.github.microwww.redis.logger.Logger;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface Run {
    void run() throws Exception;

    static void ignoreException(Logger logger, Run running) {
        try {
            running.run();
        } catch (Exception ex) {
            logger.warn("Sever Exception : {}", ex);
        }
    }

    static void silentException(Run running) {
        try {
            running.run();
        } catch (Exception ex) {
        }
    }

    static void wrapRunTimeException(Run running, String error) {
        try {
            running.run();
        } catch (Exception ex) {
            throw new RuntimeException(error, ex);
        }
    }

    static Object wrapRunTimeException(Callable run, String error) {
        try {
            return run.call();
        } catch (Exception e) {
            throw new RuntimeException(error, e);
        }
    }

    static <T> T wrapRunTimeException(Callable run, Class<T> clazz, String error) {
        try {
            return (T) run.call();
        } catch (Exception e) {
            throw new RuntimeException(error, e);
        }
    }
}
