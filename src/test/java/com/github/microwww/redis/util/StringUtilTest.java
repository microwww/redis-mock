package com.github.microwww.redis.util;

import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringUtilTest {

    @Test
    public void antPattern() {
        Pattern pattern = StringUtil.antPattern("demo.*");
        System.out.println(pattern.pattern());
        assertTrue(pattern.matcher("demo.1").matches());
        assertFalse(pattern.matcher("demo_1").matches());
    }
}