package com.github.microwww.redis.logger;

public interface Logger {

    boolean isDebugEnabled();

    void debug(String var1, Object... var2);

    boolean isInfoEnabled();

    void info(String var1, Object... var2);

    boolean isWarnEnabled();

    void warn(String var1, Object... var2);

    boolean isErrorEnabled();

    void error(String var1, Object... var2);

}
