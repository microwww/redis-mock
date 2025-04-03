package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.protocal.AbstractOperation;
import com.github.microwww.redis.protocal.RedisOutputProtocol;
import com.github.microwww.redis.protocal.RedisRequest;

import java.io.IOException;

public class ScriptOperation extends AbstractOperation {
    // EVAL
    public void eval(RedisRequest request) throws IOException {
    }
    // EVALSHA
    public void evalsha(RedisRequest request) throws IOException {
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
    }
    // SCRIPT FLUSH
    private void script_flush(RedisRequest request) throws IOException {
    }
    // SCRIPT KILL
    private void script_kill(RedisRequest request) throws IOException {
    }
    // SCRIPT LOAD
    public void script_load(RedisRequest request) throws IOException {
    }
}
