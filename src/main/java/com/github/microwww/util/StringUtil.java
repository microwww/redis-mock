package com.github.microwww.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public abstract class StringUtil {

    public static void printCharBuffer(ByteBuffer bf) {
        try {
            if (bf.remaining() > 0) {
                System.out.print(new String(bf.array(), bf.position(), bf.remaining(), "utf8"));
            }
        } catch (UnsupportedEncodingException e) {
        }
    }
}
