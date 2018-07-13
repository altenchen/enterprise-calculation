package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.exception.DataCenterException;

public abstract interface CTFOCacheThriftDB
    extends CTFOCacheDB {
    public abstract long simpleGetTable(String paramString)
        throws DataCenterException;

    public abstract long simpleGetTable(String paramString, int paramInt)
        throws DataCenterException;
}
