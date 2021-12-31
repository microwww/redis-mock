package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.RequestParams;
import com.github.microwww.redis.exception.RequestQuitException;
import com.github.microwww.redis.logger.LogFactory;
import com.github.microwww.redis.logger.Logger;
import com.github.microwww.redis.protocal.*;
import com.github.microwww.redis.protocal.jedis.JedisOutputStream;
import com.github.microwww.redis.protocal.jedis.Protocol;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ConnectionOperation extends AbstractOperation {
    private static final Logger logger = LogFactory.getLogger(ConnectionOperation.class);

    //AUTH

    /**
     * do nothing, return OK
     *
     * @param request RedisRequest
     * @throws IOException IOException
     */
    public void auth(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
    }

    //ECHO
    public void echo(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        byte[] echo = request.getParams()[0].getByteArray();
        request.getOutputProtocol().writer(echo);
    }

    public static byte[][] SERVER_META_INFO = new byte[][]{
            "server".getBytes(StandardCharsets.UTF_8),
            "redis".getBytes(StandardCharsets.UTF_8),
            "version".getBytes(StandardCharsets.UTF_8),
            "6.0.0".getBytes(StandardCharsets.UTF_8),
            "proto".getBytes(StandardCharsets.UTF_8),
            "2".getBytes(StandardCharsets.UTF_8),
            "id".getBytes(StandardCharsets.UTF_8),
            "3".getBytes(StandardCharsets.UTF_8),
            "mode".getBytes(StandardCharsets.UTF_8),
            "standalone".getBytes(StandardCharsets.UTF_8),
            "role".getBytes(StandardCharsets.UTF_8),
            "master".getBytes(StandardCharsets.UTF_8),
            "modules".getBytes(StandardCharsets.UTF_8),
            new byte[0],
    };

    //HELLO
    // HELLO [protover [AUTH username password] [SETNAME clientname]]
    public void hello(RedisRequest request) throws IOException {
        int i = 0;
        if (request.getParams().length >= 1) {
            RequestParams param = request.getParams()[i];
            int ver = param.byteArray2int();
            logger.debug("REPS protocol version is `{}`", ver);
            if (ver == 2) {
                JedisOutputStream out = request.getContext().getProtocol().getOut();
                request.getContext().setProtocol(new RespV2(out));
            } else if (ver == 3) {
                JedisOutputStream out = request.getContext().getProtocol().getOut();
                request.getContext().setProtocol(new RespV3(out));
            } else {
                logger.warn("NOPROTO sorry this protocol version is not supported : " + ver);
                request.getOutputProtocol().writerError(RedisOutputProtocol.Level.NOPROTO, "sorry this protocol version is not supported");
                return;
            }
            i++;
        }
        while (request.getParams().length > i) {
            RequestParams param = request.getParams()[i];
            i++;
            if ("AUTH".equalsIgnoreCase(param.getByteArray2string())) {
                request.expectArgumentsCountGE(i + 2);
                // RequestParams username = request.getParams()[i++];
                // RequestParams password = request.getParams()[i++];
            } else if ("SETNAME".equalsIgnoreCase(param.getByteArray2string())) {
                request.expectArgumentsCountGE(i + 1);
                RequestParams clientname = request.getParams()[i++];
                request.getSessions().setName(clientname.getByteArray2string());
            } else {
                throw new IllegalArgumentException("Arguments error");
            }
        }
        request.getOutputProtocol().writerMulti(SERVER_META_INFO);
    }

    //PING
    public void ping(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        request.getOutputProtocol().writer("PONG");
    }

    //QUIT
    public void quit(RedisRequest request) throws IOException {
        request.expectArgumentsCount(0);
        request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
        request.getOutputProtocol().flush();
        throw new RequestQuitException();
    }

    //SELECT
    public void select(RedisRequest request) throws IOException {
        request.expectArgumentsCount(1);
        RequestParams[] args = request.getParams();
        int index = Integer.parseInt(args[0].getByteArray2string());
        int db = request.getServer().getSchema().getSize();
        if (index >= db || index < 0) {
            request.getOutputProtocol().writerError(RedisOutputProtocol.Level.ERR, "DB index is out of range");
        } else {
            request.getSessions().setDatabase(index);
            request.getOutputProtocol().writer(Protocol.Keyword.OK.name());
        }
    }
}
