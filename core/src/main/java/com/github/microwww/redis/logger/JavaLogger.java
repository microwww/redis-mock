package com.github.microwww.redis.logger;

import java.util.logging.Level;
import java.util.logging.LogRecord;

public class JavaLogger implements Logger {
    public static final Level DEBUG = Level.FINE;
    java.util.logging.Logger logger;

    public JavaLogger(String name) {
        logger = java.util.logging.Logger.getLogger(name);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isLoggable(DEBUG);
    }

    @Override
    public void debug(String var1, Object... var2) {
        if (this.isDebugEnabled()) {
            var1 = build(var1, var2);
            LogRecord rc = new LogRecord(DEBUG, var1);
            rc.setParameters(var2);
            rc.setThrown(LogFactory.getThrowable(var2));
            logger.log(rc);
        }
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    @Override
    public void info(String var1, Object... var2) {
        if (this.isInfoEnabled()) {
            var1 = build(var1, var2);
            LogRecord rc = new LogRecord(Level.INFO, var1);
            rc.setParameters(var2);
            rc.setThrown(LogFactory.getThrowable(var2));
            logger.log(rc);
        }
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    @Override
    public void warn(String var1, Object... var2) {
        if (this.isWarnEnabled()) {
            var1 = build(var1, var2);
            LogRecord rc = new LogRecord(Level.WARNING, var1);
            rc.setParameters(var2);
            rc.setThrown(LogFactory.getThrowable(var2));
            logger.log(rc);
        }
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    @Override
    public void error(String var1, Object... var2) {
        if (this.isErrorEnabled()) {
            var1 = build(var1, var2);
            LogRecord rc = new LogRecord(Level.SEVERE, var1);
            rc.setParameters(var2);
            rc.setThrown(LogFactory.getThrowable(var2));
            logger.log(rc);
        }
    }

    public static String build(String format, Object... param) {
        if (param == null) {
            return format;
        }
        StringBuilder sp = new StringBuilder();
        for (int i = 0, index = 0; i < param.length; i++) {
            int of = format.indexOf("{}", index);
            if (of < 0) {
                break;
            }
            sp.append(format, index, of).append("{").append(i).append("}");
            index = of + 2;
        }
        return sp.toString();
    }
}
