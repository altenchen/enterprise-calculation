package com.ctfo.datacenter.cache.conn;

import redis.clients.jedis.ShardedJedis;

public class ReturnHandle<T> {
    private T result;
    private ShardedJedis shardedJedis;
    private String message;
    private int status = 0;

    public T getResult() {
        return (T) this.result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public ShardedJedis getShardedJedis() {
        return this.shardedJedis;
    }

    public void setShardedJedis(ShardedJedis shardedJedis) {
        this.shardedJedis = shardedJedis;
    }

    public String getMessage() {
        return this.message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getStatus() {
        return this.status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
