package com.github.microwww.database;

import com.github.microwww.util.Assert;

public class Schema {
    public static final int DEFAULT_SCHEMA_SIZE = 16;
    private final int size;
    private final RedisDatabase[] redisDatabases;

    private static final Schema def = new Schema();

    public static Schema getDef() {
        return def;
    }

    public Schema() {
        this(DEFAULT_SCHEMA_SIZE);
    }

    public Schema(int size) {
        Assert.isTrue(size > 0, "Database SIZE > 0");
        this.size = size;
        this.redisDatabases = new RedisDatabase[size];
        init();
    }

    private void init() {
        for (int i = 0; i < this.redisDatabases.length; i++) {
            redisDatabases[i] = new RedisDatabase();
        }
    }

    public RedisDatabase getRedisDatabases(int i) {
        return redisDatabases[i];
    }

    public int getSize() {
        return size;
    }
}
