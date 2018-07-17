package com.ctfo.datacenter.cache.conn;

import com.ctfo.datacenter.cache.conn.base.AddCacheConn;
import com.ctfo.datacenter.cache.conn.base.CounterCacheConn;
import com.ctfo.datacenter.cache.conn.base.DeleteCacheConn;
import com.ctfo.datacenter.cache.conn.base.QueryCacheConn;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

public abstract interface ConfigControl
    extends AddCacheConn, QueryCacheConn, CounterCacheConn, DeleteCacheConn {
    public abstract void initJedisPool(String paramString);

    public abstract Jedis getConnection();

    public abstract void freeConnection(Jedis paramJedis);

    public abstract void brokenConnection(Jedis paramJedis);

    public abstract void destoryConnection();

    public abstract void reConnection();

    public abstract void reConnection(String paramString);

    public abstract Boolean isHashFieldExist(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract String queryType(String paramString)
        throws DataCenterException;

    public abstract Set<Tuple> queryObjectSortedSet(String paramString)
        throws DataCenterException;

    public abstract List<String> queryHashValues(String paramString)
        throws DataCenterException;

    public abstract Set<String> keys(String paramString)
        throws DataCenterException;

    public abstract Long publish(String paramString1, String paramString2)
        throws DataCenterException;

    public abstract String flushDB()
        throws DataCenterException;

    public abstract Long expire(String paramString, int paramInt)
        throws DataCenterException;
}
