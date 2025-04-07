package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class ScriptOperationTest extends AbstractRedisTest {

    @Test
    public void evalNil(){
        Object sv = jedis.eval("return nil", 0);
        Assert.assertNull(sv);
        sv = jedis.eval(String.format("return redis.call('get', '%s')", UUID.randomUUID().toString()), 0);
        Assert.assertNull(sv);

        sv = jedis.eval("return 10", 0);
        Assert.assertEquals(sv, 10L);
    }

    @Test
    public void evalSimple(){
        String key = UUID.randomUUID().toString(), val = "Hello, Redis!";
        jedis.set(key, val);
        String rsp = jedis.get(key);
        Assert.assertEquals(rsp, val);

        Object ev = jedis.eval(String.format("return redis.call('get', '%s')", key), 0);
        Assert.assertEquals(ev, val);

        // 测试新的请求仍然可以调用
        Assert.assertEquals(jedis.get(key), val);
    }

    @Test
    public void evalWithParams(){
        //return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}
        String v = UUID.randomUUID().toString();
        Object ev = jedis.eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", 2, "k1", "k2", "v1", v);
        List<String> ls = (List)ev;
        Assert.assertEquals(ls.get(3), v);
    }

    @Test
    public void evalGlob(){
        try {
            jedis.eval("a=10");
            Assert.fail();
        } catch (Exception ex){
            Assert.assertNotNull(ex.getMessage());
        }
    }

    @Test
    public void evalLuaTypeTable(){
        Object table = jedis.eval("local function f(x) end \n" +
                "return {1, f, 'string'}");
        List<Object> list = (List<Object>) table;
        Assert.assertEquals(list.size(), 3);

        Assert.assertEquals(list.get(0), 1L);
        Assert.assertNull(list.get(1));
        Assert.assertEquals(list.get(2), "string");
    }

    @Test
    public void evalLuaTypeSimple(){
        Object vnil = jedis.eval("return nil");
        Assert.assertNull(vnil);

        Object vint = jedis.eval("return 1");
        Assert.assertEquals(vint, 1L);

        Object vdouble = jedis.eval("return 1.000000009");
        Assert.assertEquals(vdouble, 1L);

        String val = UUID.randomUUID().toString();
        Object vstring = jedis.eval(String.format("return '%s'", val));
        Assert.assertEquals(vstring, val);

        Object fun = jedis.eval("return function() end");
        Assert.assertEquals(fun, null);
    }
}