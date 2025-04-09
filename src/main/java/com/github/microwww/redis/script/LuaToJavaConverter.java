package com.github.microwww.redis.script;

import com.github.microwww.redis.util.SafeEncoder;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class LuaToJavaConverter {

    public static Object convert(LuaValue val) {
        if (val == LuaNull.NULL) {
            return null;
        } else if (val.isnil()) {
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
     * Redis 仅仅转换为数组，按照索引取得，不支持混合的 table。
     * @param table
     * @return
     */
    public static List<Object> convertTable2list(LuaTable table) {
        int l = table.length();
        List<Object> res = new ArrayList<>(l);
        for(int i = 1; i <= l; i++){ // LUA 从 1 开始计数
            LuaValue v = table.get(i);
            if(v.isnil()){
                res.add(null);
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

    public static LuaValue convert(Object obj) {
        if (obj == null) {
            return LuaNull.NULL;
        } else if (obj instanceof byte[]) {
            return LuaValue.valueOf(SafeEncoder.encode((byte[]) obj));
        } else if (obj instanceof Integer) {
            return LuaValue.valueOf((Integer) obj);
        } else if (obj instanceof Long) {
            return LuaValue.valueOf((Long) obj);
        } else if (obj instanceof Double) {
            return LuaValue.valueOf((Double) obj);
        } else if (obj instanceof Boolean) {
            return LuaValue.valueOf((Boolean) obj);
        } else if (obj instanceof String) {
            return LuaValue.valueOf((String) obj);
        } else if (obj instanceof List<?>) {
            return convertList((List<?>) obj);
        } else if (obj instanceof Map<?, ?>) {
            return convertMap((Map<?, ?>) obj);
        } else {
            return LuaValue.userdataOf(obj); // 默认转换为 Userdata
        }
    }

    private static LuaValue convertList(List<?> list) {
        LuaTable table = new LuaTable();
        for (int i = 0; i < list.size(); i++) {
            table.set(i + 1, convert(list.get(i)));
        }
        return table;
    }

    private static LuaValue convertMap(Map<?, ?> map) {
        LuaTable table = new LuaTable();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            LuaValue key = LuaValue.valueOf(entry.getKey().toString());
            LuaValue value = convert(entry.getValue());
            table.set(key, value);
        }
        return table;
    }
}