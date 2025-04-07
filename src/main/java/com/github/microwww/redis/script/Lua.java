package com.github.microwww.redis.script;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.protocal.jedis.Protocol;
import com.github.microwww.redis.protocal.jedis.RedisInputStream;
import com.github.microwww.redis.protocal.message.MultiMessage;
import com.github.microwww.redis.protocal.message.StringMessage;
import com.github.microwww.redis.protocal.message.Type;
import org.luaj.vm2.*;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Lua {
    public static final Logger log = LogFactory.getLogger(Lua.class);
    static ThreadLocal<RedisRequest> CONTEXT = new ThreadLocal<>();

    Globals globals = JsePlatform.standardGlobals();
    LuaTable redis = new LuaTable();

    public Lua() {
        MockRedis.Call call = new MockRedis.Call();
        redis.set("call", call);
        redis.set("pcall", call);
    }


    public void eval(RedisRequest request) throws IOException {
        try {
            CONTEXT.set(request);
            LuaTable env = new LuaTable();
            RequestParams[] params = request.getParams();
            String script = params[0].getByteArray2string();
            int keySize = params[1].byteArray2int();
            ArrayList<LuaValue> keys = new ArrayList<>(keySize);
            for (int i = 0; i < keySize; i++) {
                keys.add(LuaValue.valueOf(params[2 + i].getByteArray2string()));
            }
            env.set("KEYS", LuaValue.listOf(keys.toArray(new LuaValue[0])));

            int argSize = params.length - 2 - keySize;
            List<LuaValue> args = new ArrayList<>(argSize);
            for (int i = 0; i < argSize; i++) {
                args.add(LuaValue.valueOf(params[3 + keySize + i - 1].getByteArray2string()));
            }
            env.set("ARGV", LuaValue.listOf(args.toArray(new LuaValue[0])));

            env.set("redis", redis);
            env.setmetatable(createMetatable());

            LuaValue load = globals.load(script, "\r\n" + script + "\r\n\t script.lua", env);
            LuaValue result = load.call();
            writeOut(request.getOutputProtocol(), result);
        }finally {
            CONTEXT.remove();
        }
    }

    private LuaTable createMetatable(){
        LuaTable env = new LuaTable();
        LuaValue g = globals.get("_G");
        env.set("__index", g);
        env.set("__newindex", new ThreeArgFunction() {
            @Override
            public LuaValue call(LuaValue t, LuaValue key, LuaValue value) {
                throw new LuaError(String.format(
                        "Script attempted to create global variable '%s'",
                        key.tojstring()
                ));
            }
        });
        return env;
    }

    private void evalOut(RedisOutputProtocol outputProtocol, LuaValue res) throws IOException {
        if (res.isnil()) {
            outputProtocol.writerNull();
        } else if (res instanceof LuaTable){ //list case
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

    public static void writeOut(RedisOutputProtocol out, LuaValue val) throws IOException {
        if (val.isnil()) {
            out.writerNull();
        } else if (val.isstring()) {
            out.writer(val.tojstring());
        } else if (val.isint()) {
            out.writer(val.toint());
        } else if (val.isnumber()) { // Lua number -> Redis integer reply / Lua 数字转换成 Redis 整数
            out.writer(val.tolong());
        } else if (val.isboolean()) {
            out.writer(val.checkboolean()?1:0);
        } else if (val.istable()) {
            List<?> list = LuaToJavaConverter.convertTable2list(val.checktable());
            out.writerComplex(list.toArray());
        } else {
            //} else if (val.isfunction()) {
            //} else if (val.isuserdata()) {
            throw new IllegalArgumentException("Unsupported Lua type: " + val.typename());
        }
    }

    public void evalsha(RedisRequest request) throws IOException {
        eval(request);
    }

    static final public class MockRedis {

        static public class Call extends VarArgFunction {


            public Call() {
            }

            @Override
            public LuaValue invoke(Varargs varargs) {
                RedisServer redisServer = CONTEXT.get().getServer();
                ChannelContext channelContext = CONTEXT.get().getContext();

                int narg = varargs.narg();
                StringMessage[] args = new StringMessage[narg];
                for (int i = 0; i < narg; i++) {
                    try {
                        args[i] = new StringMessage(Type.ATTR, varargs.arg(i + 1).checkjstring().getBytes(Protocol.CHARSET));
                    } catch (UnsupportedEncodingException e) {
                        log.error("{}", e);
                        throw new RuntimeException(e);
                    }
                }

                MultiMessage multiMessage = new MultiMessage(Type.MULTI, args);
                RequestParams[] req = RequestParams.convert(multiMessage);

                // 替换掉输出流，最后再复原 !
                RedisOutputProtocol origin = channelContext.getProtocol();
                try {

                    ByteArrayOutputStream arr = new ByteArrayOutputStream(1 * 1024);
                    RedisOutputProtocol out = new RespLua(new JedisOutputStream(arr));

                    channelContext.setProtocol(out);
                    RedisRequest redisRequest = new RedisRequest(redisServer, channelContext, req);
                    try {
                        // 当前线程池直接运行
                        redisServer.getSchema().run(redisRequest);
                        redisRequest.getContext().getProtocol().flush();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    ByteArrayInputStream in = new ByteArrayInputStream(arr.toByteArray());
                    RedisInputStream redisInputStream = new RedisInputStream(in);
                    LuaValue luaValue = encodeObject(Protocol.read(redisInputStream));

                    return luaValue;
                } finally {
                    channelContext.setProtocol(origin);
                }
            }
        }
    }

    public static LuaValue encodeObject(Object dataToEncode) {
        if(Objects.isNull(dataToEncode)){
            return LuaValue.NIL;
        }
        if (dataToEncode instanceof byte[]) {
            return encode((byte[]) dataToEncode);
        }

        if (dataToEncode instanceof Long) {
            return LuaValue.valueOf(((Long) dataToEncode).intValue());
        }

        if (dataToEncode instanceof List) {
            List arrayToDecode = (List) dataToEncode;
            List<LuaValue> returnValueArray = new ArrayList<>(arrayToDecode.size());
            for (Object arrayEntry : arrayToDecode) {
                // recursive call and add to list
                returnValueArray.add(encodeObject(arrayEntry));
            }
            return LuaValue.listOf(returnValueArray.toArray(new LuaValue[arrayToDecode.size()]));
        }

        return LuaValue.valueOf(dataToEncode.toString());
    }

    public static LuaValue encode(final byte[] data) {
        try {
            return LuaValue.valueOf(new String(data, Protocol.CHARSET));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
