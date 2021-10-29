package com.github.microwww.redis.util;

import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public abstract class StringUtil {

    private static final Logger logger = LogFactory.getLogger(StringUtil.class);

    public static void loggerBuffer(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        if (bytes.length == 0) {
            return;
        }
        buffer.get(bytes);
        StringBuilder sb = new StringBuilder();
        for (byte a : bytes) {
            switch (a) {
                case '\r':
                case '\n':
                    sb.append(" ");
                    break;
                default:
                    sb.append((char) a);
            }
        }
        logger.debug("Buffer [{}]: {}", bytes.length, sb);
    }

    public static String redisErrorMessage(Exception ex) {
        String message = ex.getMessage();
        if (message != null) {
            message = message.replaceAll("\\r", "");
        }
        return message;
    }

    /**
     * @param format
     * @return
     */
    public static Pattern antPattern(String format) {
        String que = format.replaceAll(Pattern.quote("."), "\\\\Q.\\\\E");
        que = que.replaceAll(Pattern.quote("*") + "+", ".*");
        que = que.replaceAll(Pattern.quote("?"), ".");
        return Pattern.compile(que);
    }
}
