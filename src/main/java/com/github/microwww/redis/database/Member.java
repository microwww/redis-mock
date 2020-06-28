package com.github.microwww.redis.database;

import com.github.microwww.redis.util.Assert;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;

public class Member {
    private final byte[] member;
    private final BigDecimal score;
    private HashKey key;

    public Member(byte[] member, BigDecimal score) {
        Assert.allNotNull("All not null", member, score);
        this.member = member;
        this.score = score;
    }

    private Member(BigDecimal score) {
        this.member = null;
        this.key = null;
        this.score = score;
    }

    public HashKey getKey() {
        if (member == null) throw new IllegalArgumentException("Not invoke in this object !");
        if (key == null) {
            synchronized (this) {
                if (key == null) {
                    key = new HashKey(member);
                }
            }
        }
        return key;
    }

    public byte[] getMember() {
        if (this.member == null) {
            return null;
        }
        return Arrays.copyOf(this.member, member.length);
    }

    public BigDecimal getScore() {
        return score;
    }

    public boolean scoreEQ(BigDecimal decimal) {
        return this.score.compareTo(decimal) == 0;
    }

    public boolean scoreGT(BigDecimal decimal) {
        return this.score.compareTo(decimal) > 0;
    }

    public boolean scoreLT(BigDecimal decimal) {
        return this.score.compareTo(decimal) < 0;
    }

    public boolean scoreNE(BigDecimal decimal) {
        return !this.scoreEQ(decimal);
    }

    public boolean scoreGE(BigDecimal decimal) {
        return this.score.compareTo(decimal) >= 0;
    }

    public boolean scoreLE(BigDecimal decimal) {
        return this.score.compareTo(decimal) <= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Member m = (Member) o;
        return Arrays.equals(member, m.member);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(member);
    }

    public static final Comparator<Member> COMPARATOR = (Member e1, Member e2) -> {
        int c = e1.getScore().compareTo(e2.getScore());
        if (c == 0) {
            byte[] m1 = e1.getMember(), m2 = e2.getMember();
            if (m1 == null) { // NULL 是最大值, 用于范围匹配的时候包含等于
                if (m2 == null) {
                    return 0;
                }
                return 1;
            }
            if (m2 == null) {
                return -1;
            }
            int l1 = m1.length, l2 = m2.length;
            for (int i = 0; i < l1 && i < l2; i++) {
                c = m1[i] - m2[i];
                if (c != 0) return c;
            }
            return l1 - l2;
        }
        return c;
    };

    public static Member MAX(BigDecimal score) {
        return new Member(score);
    }

    public static Member MIN(BigDecimal score) {
        return new Member(new byte[0], score);
    }
}