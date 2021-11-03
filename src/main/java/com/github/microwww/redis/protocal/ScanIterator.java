package com.github.microwww.redis.protocal;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.protocal.operation.KeyOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class ScanIterator<T> {

    protected final RedisRequest request;
    protected final int cursor;
    protected final KeyOperation.ScanParams spm;

    public ScanIterator(RedisRequest request, int offset) {
        RequestParams[] args = request.getParams();
        this.cursor = args[offset].byteArray2int();
        this.request = request;
        this.spm = parseParam(offset, args);
    }

    private KeyOperation.ScanParams parseParam(int offset, RequestParams[] args) {
        KeyOperation.ScanParams spm = new KeyOperation.ScanParams();
        for (int i = offset + 1; i < args.length; i++) {
            String op = args[i].getByteArray2string();
            KeyOperation.Scan pm = KeyOperation.Scan.valueOf(op.toUpperCase());
            i = pm.next(spm, args, i);
        }
        return spm;
    }

    public ScanIterator<T> skip(Iterator<T> iterator) {
        for (int i = 0; i < this.cursor && iterator.hasNext(); i++) { // skip
            iterator.next();
        }
        return this;
    }

    public void continueWrite(Iterator<T> iterator, Function<T, byte[]>... parses) throws IOException {
        int i = this.cursor;

        List<byte[]> list = new ArrayList<>();
        for (int j = 0; j < spm.getCount() && iterator.hasNext(); j++) {
            T next = iterator.next();
            for (Function<T, byte[]> parse : parses) {
                list.add(parse.apply(next));
            }
            i++;
        }
        int cursor = iterator.hasNext() ? i : 0;
        byte[][] arrays = list.toArray(new byte[list.size()][]);

        RedisOutputProtocol.writerNested(request.getOutputStream(), (cursor + "").getBytes(), arrays);
    }

}
