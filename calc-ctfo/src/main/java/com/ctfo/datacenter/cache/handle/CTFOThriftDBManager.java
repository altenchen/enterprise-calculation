package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.exception.DataCenterException;

public abstract interface CTFOThriftDBManager
    extends CTFODBManager {
    public abstract void initParm();

    public abstract long simpleOpenCacheDB(String paramString)
        throws DataCenterException;
}
