package com.github.microwww.redis.logger;

import junit.framework.TestCase;

public class Slf4jLoggerTest extends TestCase {

    public void testInfo() {
        new Slf4jLogger(this.getName()).info("tsss {} {}", "sdfa", "sfd");
    }
}