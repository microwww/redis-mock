package com.github.microwww;

import redis.clients.jedis.*;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;

import java.util.Map;

public class AbstractCommand implements Commands {
    @Override
    public void set(String key, String value) {

    }

    @Override
    public void set(String key, String value, String nxxx, String expx, long time) {

    }

    @Override
    public void get(String key) {

    }

    @Override
    public void exists(String key) {

    }

    @Override
    public void exists(String... keys) {

    }

    @Override
    public void del(String... keys) {

    }

    @Override
    public void type(String key) {

    }

    @Override
    public void keys(String pattern) {

    }

    @Override
    public void rename(String oldkey, String newkey) {

    }

    @Override
    public void renamenx(String oldkey, String newkey) {

    }

    @Override
    public void expire(String key, int seconds) {

    }

    @Override
    public void expireAt(String key, long unixTime) {

    }

    @Override
    public void ttl(String key) {

    }

    @Override
    public void setbit(String key, long offset, boolean value) {

    }

    @Override
    public void setbit(String key, long offset, String value) {

    }

    @Override
    public void getbit(String key, long offset) {

    }

    @Override
    public void setrange(String key, long offset, String value) {

    }

    @Override
    public void getrange(String key, long startOffset, long endOffset) {

    }

    @Override
    public void move(String key, int dbIndex) {

    }

    @Override
    public void getSet(String key, String value) {

    }

    @Override
    public void mget(String... keys) {

    }

    @Override
    public void setnx(String key, String value) {

    }

    @Override
    public void setex(String key, int seconds, String value) {

    }

    @Override
    public void mset(String... keysvalues) {

    }

    @Override
    public void msetnx(String... keysvalues) {

    }

    @Override
    public void decrBy(String key, long integer) {

    }

    @Override
    public void decr(String key) {

    }

    @Override
    public void incrBy(String key, long integer) {

    }

    @Override
    public void incrByFloat(String key, double value) {

    }

    @Override
    public void incr(String key) {

    }

    @Override
    public void append(String key, String value) {

    }

    @Override
    public void substr(String key, int start, int end) {

    }

    @Override
    public void hset(String key, String field, String value) {

    }

    @Override
    public void hget(String key, String field) {

    }

    @Override
    public void hsetnx(String key, String field, String value) {

    }

    @Override
    public void hmset(String key, Map<String, String> hash) {

    }

    @Override
    public void hmget(String key, String... fields) {

    }

    @Override
    public void hincrBy(String key, String field, long value) {

    }

    @Override
    public void hincrByFloat(String key, String field, double value) {

    }

    @Override
    public void hexists(String key, String field) {

    }

    @Override
    public void hdel(String key, String... fields) {

    }

    @Override
    public void hlen(String key) {

    }

    @Override
    public void hkeys(String key) {

    }

    @Override
    public void hvals(String key) {

    }

    @Override
    public void hgetAll(String key) {

    }

    @Override
    public void rpush(String key, String... strings) {

    }

    @Override
    public void lpush(String key, String... strings) {

    }

    @Override
    public void llen(String key) {

    }

    @Override
    public void lrange(String key, long start, long end) {

    }

    @Override
    public void ltrim(String key, long start, long end) {

    }

    @Override
    public void lindex(String key, long index) {

    }

    @Override
    public void lset(String key, long index, String value) {

    }

    @Override
    public void lrem(String key, long count, String value) {

    }

    @Override
    public void lpop(String key) {

    }

    @Override
    public void rpop(String key) {

    }

    @Override
    public void rpoplpush(String srckey, String dstkey) {

    }

    @Override
    public void sadd(String key, String... members) {

    }

    @Override
    public void smembers(String key) {

    }

    @Override
    public void srem(String key, String... member) {

    }

    @Override
    public void spop(String key) {

    }

    @Override
    public void spop(String key, long count) {

    }

    @Override
    public void smove(String srckey, String dstkey, String member) {

    }

    @Override
    public void scard(String key) {

    }

    @Override
    public void sismember(String key, String member) {

    }

    @Override
    public void sinter(String... keys) {

    }

    @Override
    public void sinterstore(String dstkey, String... keys) {

    }

    @Override
    public void sunion(String... keys) {

    }

    @Override
    public void sunionstore(String dstkey, String... keys) {

    }

    @Override
    public void sdiff(String... keys) {

    }

    @Override
    public void sdiffstore(String dstkey, String... keys) {

    }

    @Override
    public void srandmember(String key) {

    }

    @Override
    public void zadd(String key, double score, String member) {

    }

    @Override
    public void zadd(String key, double score, String member, ZAddParams params) {

    }

    @Override
    public void zadd(String key, Map<String, Double> scoreMembers) {

    }

    @Override
    public void zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {

    }

    @Override
    public void zrange(String key, long start, long end) {

    }

    @Override
    public void zrem(String key, String... members) {

    }

    @Override
    public void zincrby(String key, double score, String member) {

    }

    @Override
    public void zincrby(String key, double score, String member, ZIncrByParams params) {

    }

    @Override
    public void zrank(String key, String member) {

    }

    @Override
    public void zrevrank(String key, String member) {

    }

    @Override
    public void zrevrange(String key, long start, long end) {

    }

    @Override
    public void zrangeWithScores(String key, long start, long end) {

    }

    @Override
    public void zrevrangeWithScores(String key, long start, long end) {

    }

    @Override
    public void zcard(String key) {

    }

    @Override
    public void zscore(String key, String member) {

    }

    @Override
    public void watch(String... keys) {

    }

    @Override
    public void sort(String key) {

    }

    @Override
    public void sort(String key, SortingParams sortingParameters) {

    }

    @Override
    public void blpop(String[] args) {

    }

    @Override
    public void sort(String key, SortingParams sortingParameters, String dstkey) {

    }

    @Override
    public void sort(String key, String dstkey) {

    }

    @Override
    public void brpop(String[] args) {

    }

    @Override
    public void brpoplpush(String source, String destination, int timeout) {

    }

    @Override
    public void zcount(String key, double min, double max) {

    }

    @Override
    public void zcount(String key, String min, String max) {

    }

    @Override
    public void zrangeByScore(String key, double min, double max) {

    }

    @Override
    public void zrangeByScore(String key, String min, String max) {

    }

    @Override
    public void zrangeByScore(String key, double min, double max, int offset, int count) {

    }

    @Override
    public void zrangeByScoreWithScores(String key, double min, double max) {

    }

    @Override
    public void zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {

    }

    @Override
    public void zrangeByScoreWithScores(String key, String min, String max) {

    }

    @Override
    public void zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {

    }

    @Override
    public void zrevrangeByScore(String key, double max, double min) {

    }

    @Override
    public void zrevrangeByScore(String key, String max, String min) {

    }

    @Override
    public void zrevrangeByScore(String key, double max, double min, int offset, int count) {

    }

    @Override
    public void zrevrangeByScoreWithScores(String key, double max, double min) {

    }

    @Override
    public void zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {

    }

    @Override
    public void zrevrangeByScoreWithScores(String key, String max, String min) {

    }

    @Override
    public void zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {

    }

    @Override
    public void zremrangeByRank(String key, long start, long end) {

    }

    @Override
    public void zremrangeByScore(String key, double start, double end) {

    }

    @Override
    public void zremrangeByScore(String key, String start, String end) {

    }

    @Override
    public void zunionstore(String dstkey, String... sets) {

    }

    @Override
    public void zunionstore(String dstkey, ZParams params, String... sets) {

    }

    @Override
    public void zinterstore(String dstkey, String... sets) {

    }

    @Override
    public void zinterstore(String dstkey, ZParams params, String... sets) {

    }

    @Override
    public void strlen(String key) {

    }

    @Override
    public void lpushx(String key, String... string) {

    }

    @Override
    public void persist(String key) {

    }

    @Override
    public void rpushx(String key, String... string) {

    }

    @Override
    public void echo(String string) {

    }

    @Override
    public void linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value) {

    }

    @Override
    public void bgrewriteaof() {

    }

    @Override
    public void bgsave() {

    }

    @Override
    public void lastsave() {

    }

    @Override
    public void save() {

    }

    @Override
    public void configSet(String parameter, String value) {

    }

    @Override
    public void configGet(String pattern) {

    }

    @Override
    public void configResetStat() {

    }

    @Override
    public void multi() {

    }

    @Override
    public void exec() {

    }

    @Override
    public void discard() {

    }

    @Override
    public void objectRefcount(String key) {

    }

    @Override
    public void objectIdletime(String key) {

    }

    @Override
    public void objectEncoding(String key) {

    }

    @Override
    public void bitcount(String key) {

    }

    @Override
    public void bitcount(String key, long start, long end) {

    }

    @Override
    public void bitop(BitOP op, String destKey, String... srcKeys) {

    }

    @Override
    public void scan(int cursor, ScanParams params) {

    }

    @Override
    public void hscan(String key, int cursor, ScanParams params) {

    }

    @Override
    public void sscan(String key, int cursor, ScanParams params) {

    }

    @Override
    public void zscan(String key, int cursor, ScanParams params) {

    }

    @Override
    public void scan(String cursor, ScanParams params) {

    }

    @Override
    public void hscan(String key, String cursor, ScanParams params) {

    }

    @Override
    public void sscan(String key, String cursor, ScanParams params) {

    }

    @Override
    public void zscan(String key, String cursor, ScanParams params) {

    }

    @Override
    public void waitReplicas(int replicas, long timeout) {

    }

    @Override
    public void bitfield(String key, String... arguments) {

    }
}
