# Spring-boot-mocker-redis
This is a spring-boot-starter. if you dependency a redis-server, Using it, you can run test case, But do not need a real redis-server.

Run it for your test, ONLY !

This project is moved from [spring-boot-mocker-redis](https://github.com/microwww/spring-boot-mocker-redis)

## Spring-boot-mocker-redis
Using it, you can add the maven dependency :
```
<dependency>
	<groupId>com.github.microwww</groupId>
	<artifactId>mocker-redis-spring-boot-starter</artifactId>
	<version>5.3.0</version>
</dependency>
```

You can set `mocker.jedis.enable=false` to disable it, like any spring-boot project config.

The embedded redis server will listener `spring.redis.port` port, and host is `0.0.0.0`, so you must set `spring.redis.host` to match this host.

## Version
ALL version to see [maven repository](https://mvnrepository.com/artifact/com.github.microwww/mocker-redis-spring-boot-starter)

## Dependency

vresion 2.0.0 + , it depends on java redis server: [jedis-mock](https://github.com/microwww/redis-mock) , Last version is v0.3.0
```
<dependency>
	<groupId>com.github.microwww</groupId>
	<artifactId>redis-server</artifactId>
	<version>${jedis-mock.version}</version>
</dependency>
```
