package com.github.microwww.redis.script;

import org.luaj.vm2.LuaNil;
import org.luaj.vm2.LuaValue;

public final class LuaNull extends LuaValue {

    public static final LuaNull NULL = new LuaNull();
    public static final LuaValue nil = LuaNil.NIL;

    @Override
    public int type() {
        return nil.type();
    }

    @Override
    public String typename() {
        return "null";
    }

    @Override
    public LuaValue eq(LuaValue val) {
        return nil.eq(val);
    }

    @Override
    public boolean eq_b(LuaValue val) {
        return nil.eq_b(val);
    }

    @Override
    public boolean equals(Object obj) {
        return nil.equals(obj);
    }
}
