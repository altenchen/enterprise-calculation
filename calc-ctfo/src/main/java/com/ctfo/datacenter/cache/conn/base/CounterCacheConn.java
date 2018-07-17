package com.ctfo.datacenter.cache.conn.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

public abstract interface CounterCacheConn {
    public abstract Long incr(String paramString)
        throws DataCenterException;

    public abstract Long incr(String paramString, int paramInt)
        throws DataCenterException;

    public abstract Long decr(String paramString)
        throws DataCenterException;

    public abstract Long decr(String paramString, int paramInt)
        throws DataCenterException;
}
