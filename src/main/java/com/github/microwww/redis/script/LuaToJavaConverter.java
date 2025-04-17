package com.github.microwww.redis.script;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class LuaToJavaConverter {
    public static Object convert(LuaValue val) {
        if (val.isnil()) {
            return null;
        } else if (val.isboolean()) {
            return val.toboolean();
        } else if (val.isint()) {
            return val.toint();
        } else if (val.islong()) {
            return val.tolong();
        } else if (val.isnumber()) {
            return val.todouble();
        } else if (val.isstring()) {
            return val.toString();
        } else if (val.istable()) {
            return convertTable2list(val.checktable());
        } else if (val.isfunction()) {
            return val;  // 返回 LuaFunction，供后续调用
        } else if (val.isuserdata()) {
            return val.checkuserdata();  // 返回原始 Java 对象
        } else {
            throw new IllegalArgumentException("Unsupported Lua type: " + val.typename());
        }
    }

    /**
     * Redis 仅仅转换为数组，按照索引取得，对于混合的 table，找到第一个 nil 就结束
     * @param table
     * @return
     */
    public static List<Object> convertTable2list(LuaTable table) {
        int l = table.length();
        List<Object> res = new ArrayList<>(l);
        for(int i = 1; i <= l; i++){ // LUA 从 1 开始计数
            LuaValue v = table.get(i);
            if(v.isnil()){
                break;
            }
            res.add(convert(v));
        }
        return res;
    }

    public static Map<Object, Object> convertTable2map(LuaTable table) {
        Map<Object, Object> resultMap = new HashMap<>();
        for (LuaValue key : table.keys()) {
            LuaValue value = table.get(key);
            resultMap.put(convert(key), convert(value));
        }
        return resultMap;
    }
}