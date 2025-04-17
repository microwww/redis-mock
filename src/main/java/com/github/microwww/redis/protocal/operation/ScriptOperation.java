package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.Protocol;
import com.github.microwww.redis.protocal.message.StringMessage;
import com.github.microwww.redis.script.Lua;
import com.github.microwww.redis.script.NoScript;
import com.github.microwww.redis.script.RespLua;
import com.github.microwww.redis.script.Script;
import com.github.microwww.redis.util.SafeEncoder;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kill 总是返回 OK， 不做 kill 操作 ！
 */
public class ScriptOperation extends AbstractOperation {
    private static Logger log = LogFactory.getLogger(ScriptOperation.class);
    protected Script lua;
    {
        try{
            lua = new Lua();
        } catch(NoClassDefFoundError ex){
            log.warn("找不到 Lua 的脚本执行环境，如果不需要Lua支持，请忽略该提示，否则请添加依赖: org.luaj:luaj-jse:^3.0.1");
            lua = new NoScript("Add dependency org.luaj:luaj-jse:^3.0.1");
        }
    }

    protected Map<String, String> scripts = new ConcurrentHashMap<>();
    private AtomicInteger running = new AtomicInteger(0);

    // EVAL
    public void eval(RedisRequest request) throws IOException {
        RedisOutputProtocol origin = request.getContext().getProtocol();
        RespLua resp = RespLua.create();
        try {
            running.incrementAndGet();
            request.getContext().setProtocol(resp);
            lua.eval(request);
            request.getOutputProtocol().flush();
        } finally {
            running.decrementAndGet();
            request.getContext().setProtocol(origin);
        }
        origin.getOut().write(resp.getData());
        origin.flush();
    }
    private void evalOut(RedisOutputProtocol outputProtocol, LuaValue res) throws IOException {
        if (res.isnil()) {
            outputProtocol.writerNull();
        }else if (res instanceof LuaTable){ //list case
            LuaTable resTable = (LuaTable) res;
            LuaValue len = resTable.len();
            if (len.isint()) {
                int dataLen = len.toint();
                byte[][] bytes = new byte[dataLen][];

                for (int j = 0; j < dataLen; j++) {
                    LuaValue luaValue = resTable.get(j+1);
                    if (luaValue.isstring()) {
                        bytes[j] = luaValue.tojstring().getBytes(Protocol.CHARSET);
                    } else {
                        bytes[j] = new byte[0];
                    }
                }
                outputProtocol.writerMulti(bytes);
            }
        }else if (res.islong()){
            outputProtocol.writer(res.tolong());
        }else {
            outputProtocol.writer(res.tojstring());
        }
    }
    // EVALSHA
    public void evalsha(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(2);
        RequestParams[] pm = request.getParams();
        String hash = pm[0].getByteArray2string();
        String script = scripts.get(hash);
        if(script == null){
            String err = String.format("NOSCRIPT No matching script [%s]. Please use EVAL.", hash);
            request.getOutputProtocol().writerError(RedisOutputProtocol.Level.ERR, err);
        }
        RequestParams[] params = new RequestParams[pm.length];
        params[0] = new RequestParams(new StringMessage(pm[0].getOrigin().type, SafeEncoder.encode(script)));
        System.arraycopy(pm, 1, params, 1, pm.length - 1);
        RedisRequest req = new RedisRequest(request.getServer(), request.getContext(), script, params);
        this.eval(req);
    }
    // SCRIPT EXISTS
    public void script(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        String cmd = request.getParams()[0].getByteArray2string().toLowerCase();
        switch (cmd){
            case "exists":{
                this.script_exists(request);
                break;
            }
            case "flush":{
                this.script_flush(request);
                break;
            }
            case "kill":{
                this.script_kill(request);
                break;
            }
            case "load":{
                this.script_load(request);
                break;
            }
            default:{
                request.getOutputProtocol().writerError(
                        RedisOutputProtocol.Level.ERR,
                        String.format("ERR Unknown subcommand or wrong number of arguments for '%s'. Try SCRIPT HELP.", cmd)
                );
            }
        }
    }
    // SCRIPT EXISTS
    private void script_exists(RedisRequest request) throws IOException {
        request.expectArgumentsCountGE(1);
        RequestParams[] params = request.getParams();
        Object[] res = Arrays.stream(params, 1, params.length)
                .map(e -> e.getByteArray2string())
                .map(e -> scripts.get(e))
                .map(e -> e == null ? 0 : 1)
                .toArray();
        request.getOutputProtocol().writerComplex(res);
    }
    // SCRIPT FLUSH
    private void script_flush(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        scripts.clear();
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
    }
    // SCRIPT KILL
    private void script_kill(RedisRequest request) throws IOException {
        // NOTBUSY No scripts in execution right now.
        if(running.get() == 0){
            request.getOutputProtocol().writerError(RedisOutputProtocol.Level.ERR, "NOTBUSY No scripts in execution right now.");
        } else {
            request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
        }
    }
    // SCRIPT LOAD
    public void script_load(RedisRequest request) throws IOException {
        request.expectArgumentsCount(2);
        String script = request.getParams()[1].getByteArray2string();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            byte[] bt = digest.digest(SafeEncoder.encode(script));
            String hash = new BigInteger(1, bt).toString(16);
            scripts.put(hash, script);
            request.getOutputProtocol().writer(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }
}
