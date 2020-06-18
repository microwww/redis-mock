package com.github.microwww.database;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataList extends AbstractValueData<List<byte[]>> {
    private final List<byte[]> origin;

    public DataList() {
        this.origin = new CopyOnWriteArrayList<>();
        this.data = Collections.unmodifiableList(this.origin);
    }
    //BLPOP
    //BRPOP
    //BRPOPLPUSH

    //LINDEX
    public synchronized Optional<byte[]> getByIndex(int index) {
        int max = origin.size();
        index = index(index);
        if (index >= max) {
            return Optional.empty();
        }
        return Optional.ofNullable(this.origin.get(index));
    }

    //LINSERT
    public synchronized boolean findAndOffsetInsert(byte[] pivot, int offset, byte[] value) {
        int index = this.indexOf(pivot);
        if (index >= 0) {
            this.origin.add(index + offset, value);
            return true;
        }
        return false;
    }

    public synchronized int indexOf(byte[] val) {
        int index = -1;
        for (int i = 0; i < this.origin.size(); i++) {
            byte[] bytes = this.origin.get(i);
            if (Arrays.equals(bytes, val)) {
                index = i;
                break;
            }
        }
        return index;
    }

    public synchronized byte[] remove(int index) {
        return origin.remove(index);
    }

    //LLEN
    //LPOP
    public synchronized byte[] leftPop() {
        return origin.remove(0);
    }

    //LPUSH
    public synchronized void leftAdd(byte[]... bytes) {
        for (byte[] a : bytes) {
            origin.add(0, a);
        }
    }

    //LPUSHX
    //LRANGE
    public synchronized byte[][] range(int from, int includeTo) {
        int max = origin.size();
        from = index(from);
        int to = index(includeTo) + 1;
        if (from >= max) {
            return new byte[0][];
        }
        int end = Math.min(max, to);
        byte[][] byt = new byte[end - from][];
        for (int i = 0; i < byt.length; i++) {
            byt[i] = this.origin.get(from + i);
        }
        return byt;
    }

    public synchronized int index(int index) {
        if (index < 0) {
            return this.origin.size() + index;
        }
        return index;
    }

    //LREM key count value
    public synchronized int remove(int count, byte[] val) {
        int ct = 0;
        if (count > 0) {
            int size = this.origin.size();
            for (int i = 0; i < size; i++) {
                if (Arrays.equals(val, this.origin.get(i))) {
                    this.origin.remove(i);
                    count--;
                    if (count <= 0) {
                        break;
                    }
                    i--;
                    size--;
                    ct++;
                }
            }
        } else {
            count = Math.abs(count);
            int size = this.origin.size();
            if (count == 0) {
                count = size;
            }
            for (int i = size - 1; i >= 0; i--) {
                if (Arrays.equals(val, this.origin.get(i))) {
                    this.origin.remove(i);
                    count--;
                    ct++;
                    if (count <= 0) {
                        break;
                    }
                }
            }
        }
        return ct;
    }

    //LSET
    public synchronized byte[] set(int index, byte[] element) {
        return origin.set(index, element);
    }

    //LTRIM
    public synchronized void trim(int start, int includeStop) {
        int from = index(start);
        int to = index(includeStop) + 1;
        int max = this.origin.size();
        if (from >= max || from > to) {
            this.origin.clear();
            return;
        }
        List<byte[]> bytes = this.origin.subList(from, Math.min(max, to));
        this.origin.clear();
        this.origin.addAll(bytes);
    }

    //RPOP
    public synchronized byte[] rightPop() {
        return origin.remove(origin.size() - 1);
    }

    //RPOPLPUSH
    //RPUSH
    public synchronized void rightAdd(byte[]... bytes) {
        this.origin.addAll(Arrays.asList(bytes));
    }
    //RPUSHX
}
