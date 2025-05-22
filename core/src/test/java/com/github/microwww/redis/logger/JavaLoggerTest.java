package com.github.microwww.redis.logger;

import org.junit.Assert;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaLoggerTest {

    @Test
    public void testInfo() {
        Logger logger = Logger.getLogger(this.getClass().getName());
        logger.log(Level.INFO, "test {0}, {1}", new Object[]{"1", "1", new RuntimeException()});
    }

    @Test
    public void testBuild() {
        String build = JavaLogger.build("test {}, {}:{}", "t", "t", "t");
        Assert.assertEquals("test {0}, {1}:{2}", build);
    }

    @Test
    public void testServer() {
        JavaLogger test = new JavaLogger("test");
        test.error("this is a error ! {}, {}", "a", "b", new RuntimeException());
    }
}