package com.github.microwww.redis.protocal.operation;

import com.github.microwww.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ScriptOperationTest extends AbstractRedisTest {

    @Test
    public void evalNil(){
        Object sv = jedis.eval("return nil", 0);
        Assert.assertNull(sv);
        sv = jedis.eval(String.format("return redis.call('get', '%s')", UUID.randomUUID()), 0);
        Assert.assertNull(sv);

        sv = jedis.eval("return 10", 0);
        Assert.assertEquals(sv, 10L);
    }

    @Test
    public void nil(){
        Object res;
        // function (nil)
        res = jedis.eval("" +
                "local function direct(v) \n" +
                "  return v               \n" +
                "end                      \n" +
                "return direct(nil)", 0);
        Assert.assertNull(res);

        // nil equal
        res = jedis.eval("" +
                "local function direct(v) \n" +
                "  return v == nil        \n" +
                "end                      \n" +
                "return direct(nil)", 0);
        Assert.assertEquals(res, 1L);

        // talbe 多个 nil 会忽略
        String k0 = UUID.randomUUID().toString();
        String k1 = UUID.randomUUID().toString();
        List list = (List) jedis.eval("return redis.call('mget', KEYS[1], ARGV[1])", 1, k0, k1);
        Assert.assertEquals(list.size(), 2);
        Assert.assertNull(list.get(0));
        Assert.assertNull(list.get(1));
        list = (List) jedis.eval("" +
                "local t = redis.call('mget', KEYS[1], ARGV[1]) \n" +
                "return {0, t[0], nil, t[1], 1}", 1, k0, k1);
        System.out.println(list);
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
    public void evalLuaEval() {
        String key = UUID.randomUUID().toString();
        String val = UUID.randomUUID().toString();
        jedis.set(key, val);
        Object v = jedis.eval(String.format("return redis.call('get', '%s')", key));
        Assert.assertEquals(v, val);
        Object vnil = jedis.eval(String.format("return redis.call('get', '%s')", val));
        Assert.assertNull(vnil);
        List list = (List) jedis.eval("local v = redis.call('mget', KEYS[1], ARGV[1]) \n" +
                "print(#v) \n" +
                "return v", 1, key, val, key);
        Assert.assertEquals(val, list.get(0));
        Assert.assertNull(list.get(1));
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

    @Test
    public void script_load() {
        String hash = jedis.scriptLoad("return 'hello redis !'");
        Assert.assertEquals(hash, "8748eea8ab3f9fa57fc34c9aeed7655f7f0b9c48");
    }

    @Test
    public void evalsha(){
        String v = UUID.randomUUID().toString();
        String hash = jedis.scriptLoad(String.format("return '%s'", v));
        Object r = jedis.evalsha(hash);
        Assert.assertEquals(v, r);
    }

    @Test
    public void script_exists() {
        String v = UUID.randomUUID().toString();
        String hash = jedis.scriptLoad(String.format("return '%s'", v));
        List<Boolean> exists = jedis.scriptExists("no-exist-1", "no-exist-2", hash, "no-exist-3");
        Assert.assertEquals(exists, Arrays.asList(false, false, true, false));
    }

    @Test
    public void script_flush(){
        String v = UUID.randomUUID().toString();
        String hash = jedis.scriptLoad(String.format("return '%s'", v));
        Boolean ex = jedis.scriptExists(hash);
        Assert.assertTrue(ex.booleanValue());
        jedis.scriptFlush();
        ex = jedis.exists(hash);
        Assert.assertFalse(ex);
    }

    @Test
    public void script_kill(){
        try {
            String v = jedis.scriptKill();
            Assert.assertEquals(v,"OK");
        } catch (Exception ex){
            Assert.assertTrue(ex.getMessage().contains("No scripts in execution"));
        }
    }
}