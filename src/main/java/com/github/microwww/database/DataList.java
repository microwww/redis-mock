package com.github.microwww.database;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataList extends AbstractValueData<List<byte[]>> {
    public DataList() {
        this.data = new CopyOnWriteArrayList<>();
    }
}
