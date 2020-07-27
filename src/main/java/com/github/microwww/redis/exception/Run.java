package com.github.microwww.redis.exception;

import com.github.microwww.redis.logger.Logger;

@FunctionalInterface
public interface Run {
    void run() throws Exception;

    static void ignoreException(Logger logger, Run warnLogger) {
        try {
            warnLogger.run();
        } catch (Exception ex) {
            logger.warn("Sever Exception : {}", ex);
        }
    }
}
