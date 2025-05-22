package com.github.microwww.redis.script;

import java.io.IOException;

import com.github.microwww.redis.protocal.RedisRequest;
import com.github.microwww.redis.protocal.RedisOutputProtocol.Level;

public class NoScript implements Script {

    protected String error ;

    public NoScript(String error){
        this.error = error;
    }

    public void eval(RedisRequest request) throws IOException{
        request.getContext().getProtocol().writerError(Level.ERR, this.error);
    }

}
