package com.github.microwww.redis.util;

import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.regex.Pattern;

public abstract class StringUtil {

    public static void loggerBuffer(Logger log, ByteBuffer buffer) {
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
        log.debug("Buffer [{}]: {}", bytes.length, sb);
    }

    public static String redisErrorMessage(Exception ex) {
        String message = ex.getMessage();
        if (message != null) {
            message = message.replaceAll("\\r", "");
        }
        return message;
    }

    /**
     * @param format apache-ant
     * @return Pattern
     */
    public static Pattern antPattern(String format) {
        String que = format.replaceAll(Pattern.quote("."), "\\\\Q.\\\\E");
        que = que.replaceAll(Pattern.quote("*") + "+", ".*");
        que = que.replaceAll(Pattern.quote("?"), ".");
        return Pattern.compile(que);
    }

    public static boolean antPatternMatches(String patten, String str) {
        return antPattern(patten).matcher(str).matches();
    }

    public static String remoteHost(SocketChannel channel) {
        try {
            InetSocketAddress rm = (InetSocketAddress) channel.getRemoteAddress();
            return rm.getHostName() + ":" + rm.getPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
