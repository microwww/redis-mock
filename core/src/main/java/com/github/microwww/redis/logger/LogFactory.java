package com.github.microwww.redis.logger;

public class LogFactory {

    private static LoggerImpl imp;

    // log4j2
    // Class.forName("org.apache.logging.log4j.core.config.Configurator");
    // log4j
    // Class.forName("org.apache.log4j.Level");
    // logback
    // Class.forName("ch.qos.logback.classic.LoggerContext");

    static {
        for (LoggerImpl value : LoggerImpl.values()) {
            if (value.isSupport()) {
                imp = value;
                break;
            }
        }
    }

    public static Logger getLogger(String name) {
        return imp.create(name);
    }

    public static Logger getLogger(Class clazz) {
        return imp.create(clazz.getName());
    }

    public static Throwable getThrowable(Object... params) {
        for (int i = params.length - 1; i >= 0; i--) {
            Object p = params[i];
            if (p instanceof Throwable) {
                return (Throwable) p;
            }
        }
        return null;
    }

    public enum LoggerImpl {
        SLF4J {
            @Override
            public Logger create(String name) {
                return new Slf4jLogger(name);
            }

            @Override
            public boolean isSupport() {
                try {
                    Class.forName("org.slf4j.LoggerFactory");
                    return true;
                } catch (Throwable e) {//
                    return false;
                }
            }
        },
        JAVA {
            @Override
            public Logger create(String name) {
                return new JavaLogger(name);
            }

            @Override
            public boolean isSupport() {
                return true;
            }
        };

        public abstract Logger create(String name);

        public abstract boolean isSupport();
    }
}
