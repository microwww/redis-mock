package com.github.microwww.redis.protocal.operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.database.Bytes;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.Protocol;

// By hash-map not log-log
public class HyperLogLog extends AbstractOperation {

    private Map<Bytes, Set<Bytes>> data = new HashMap<>();

    // PFadd : key element [element ...]
    public void pfadd(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        Bytes key = request.getParams()[0].toBytes();
        if (!data.containsKey(key)) {
            data.put(key, new HashSet<>());
        }
        Set<Bytes> s = data.get(key);
        RequestParams[] req = request.getParams();
        for (int i = 1; i < req.length; i++) {
            s.add(req[i].toBytes());
        }
        request.getOutputProtocol().writer(1);
    }

    // PFCOUNT key [key ...]
    public void pfcount(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        Set<Bytes> set = new HashSet<>();
        RequestParams[] req = request.getParams();
        for (int i = 0; i < req.length; i++) {
            Set<Bytes> v = data.get(req[i].toBytes());
            if (v != null) {
                set.addAll(v);
            }
        }
        request.getOutputProtocol().writer(set.size());
    }

    // PFMERGE destkey sourcekey [sourcekey ...]
    public void pfmerge(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        Bytes key = request.getParams()[0].toBytes();
        Set<Bytes> set = data.get(key);
        if (set == null) {
            set = new HashSet<>();
            data.put(key, set);
        }
        RequestParams[] req = request.getParams();
        for (int i = 1; i < req.length; i++) {
            Set<Bytes> v = data.get(req[i].toBytes());
            if (v != null) {
                set.addAll(v);
            }
        }
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
    }

}
