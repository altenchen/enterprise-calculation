package com.ctfo.datacenter.cache.conn;

import com.ctfo.datacenter.cache.conn.base.AddTimeOutCacheConn;
import com.ctfo.datacenter.cache.conn.base.CounterCacheConn;
import com.ctfo.datacenter.cache.conn.base.DeleteCacheConn;
import com.ctfo.datacenter.cache.conn.base.QueryCacheConn;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.Tuple;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract interface ShardControl
    extends AddTimeOutCacheConn, QueryCacheConn, DeleteCacheConn, CounterCacheConn {
    public abstract void initJedisPool(Map<String, String> paramMap, ConnConfig paramConnConfig);

    public abstract void reConnection(Map<String, String> paramMap);

    public abstract ShardedJedis getConnection();

    public abstract void freeConnection(ShardedJedis paramShardedJedis);

    public abstract void brokenConnection(ShardedJedis paramShardedJedis);

    public abstract void destoryPool();

    public abstract Set<Tuple> queryObjectSortedSet(String paramString)
        throws DataCenterException;

    public abstract String queryType(String paramString)
        throws DataCenterException;

    public abstract Collection<Jedis> queryAllShards()
        throws DataCenterException;

    public abstract String queryAddress(String paramString)
        throws DataCenterException;

    public abstract ShardPipelineControl getPipelineInstance()
        throws DataCenterException;
}
