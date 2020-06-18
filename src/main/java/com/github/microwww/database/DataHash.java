package com.github.microwww.database;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataHash extends AbstractValueData<Map<HashKey, byte[]>> {

    public DataHash() {
        this.data = new ConcurrentHashMap<>();
    }
}
