package com.github.microwww.redis.util;

import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class StringUtilTest {

    @Test
    public void antPattern() {
        {
            Pattern pattern = StringUtil.antPattern("demo.*");
            // System.out.println(pattern.pattern());
            assertTrue(pattern.matcher("demo.1").matches());
            assertFalse(pattern.matcher("demo_1").matches());
        }
        {
            Pattern pattern = StringUtil.antPattern("demo.**");
            assertEquals("demo\\Q.\\E.*", pattern.pattern());
        }
        {
            Pattern pattern = StringUtil.antPattern("demo.?.test");
            // System.out.println(pattern.pattern());
            assertTrue(pattern.matcher("demo.1.test").matches());
            assertTrue(pattern.matcher("demo.a.test").matches());
            assertFalse(pattern.matcher("demo.ab.test").matches());
        }
    }
}