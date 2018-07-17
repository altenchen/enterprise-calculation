package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.Set;

public abstract interface CTFOCacheDB {
    public abstract CTFOCacheTable getTable(String paramString, int paramInt)
        throws DataCenterException;

    public abstract CTFOCacheTable getTable(String paramString)
        throws DataCenterException;

    public abstract Boolean isTableExist(String paramString)
        throws DataCenterException;

    public abstract Set<String> getTableNames()
        throws DataCenterException;

    public abstract long getTableSize()
        throws DataCenterException;

    public abstract long deleteTable(String paramString)
        throws DataCenterException;
}
