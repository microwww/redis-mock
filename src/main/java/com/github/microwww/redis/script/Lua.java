package com.github.microwww.redis.script;

import com.github.microwww.redis.ChannelContext;
import com.github.microwww.redis.RedisServer;
import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.*;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.protocal.jedis.Protocol;
import com.github.microwww.redis.protocal.jedis.RedisInputStream;
import com.github.microwww.redis.protocal.message.MultiMessage;
import com.github.microwww.redis.protocal.message.StringMessage;
import com.github.microwww.redis.protocal.message.Type;
import org.luaj.vm2.*;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Lua {
    public static final Logger log = LogFactory.getLogger(Lua.class);

    Globals globals = JsePlatform.standardGlobals();
    LuaValue coerce = CoerceJavaToLua.coerce(new MockRedis());


    {
        globals.set("redis",coerce);
    }


    public void eval(RedisRequest request) throws IOException {
        try {
            LuaTable t = new LuaTable();
            MockRedis.Call call = new MockRedis.Call(request.getServer(), request.getContext());
            t.set("call", call);
            t.set("pcall", call);
            t.set("__index", t);
            coerce.setmetatable(t);

            RequestParams[] params = request.getParams();
            String script = params[0].getByteArray2string();
            int keySize = params[1].byteArray2int();
            ArrayList<LuaValue> keys = new ArrayList<>(keySize);
            for (int i = 0; i < keySize; i++) {
                keys.add(LuaValue.valueOf(params[2 + i].getByteArray2string()));
            }
            globals.set("KEYS", LuaValue.listOf(keys.toArray(new LuaValue[0])));

            int argSize = params.length - 2 - keySize;
            ArrayList<LuaValue> args = new ArrayList<>(argSize);
            for (int i = 0; i < argSize; i++) {
                args.add(LuaValue.valueOf(params[3 + keySize + i - 1].getByteArray2string()));
            }
            globals.set("ARGV", LuaValue.listOf(args.toArray(new LuaValue[0])));

            LuaValue load = globals.load(script);
            LuaValue result = load.call();
            evalOut(request.getOutputProtocol(), result);
        }finally {
            //mockSocketChannel.clearRecorder();
        }
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

    public void evalsha(RedisRequest request) throws IOException {
        eval(request);
    }

    static final public class MockRedis {

        static public class Call extends VarArgFunction {

            RedisServer redisServer;
            ChannelContext channelContext;

            public Call(RedisServer redisServer, ChannelContext channelContext) {
                this.redisServer = redisServer;
                this.channelContext = channelContext;
            }

            @Override
            public LuaValue invoke(Varargs varargs) {
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
                    RedisOutputProtocol out = new RespV2(new JedisOutputStream(arr));
                    channelContext.setProtocol(out);
                    RedisRequest redisRequest = new RedisRequest(redisServer, channelContext, req);
                    try {
                        // 不能用新的线程池
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
