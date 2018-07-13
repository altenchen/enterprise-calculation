package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.Set;

public abstract interface CTFODBManager {
    public abstract CTFOCacheDB openCacheDB(String paramString)
        throws DataCenterException;

    public abstract Boolean isDBExist(String paramString)
        throws DataCenterException;

    public abstract Set<String> getDBNames()
        throws DataCenterException;

    public abstract long getTableSize(String paramString)
        throws DataCenterException;

    public abstract long deleteDB(String paramString)
        throws DataCenterException;

    public abstract void closeConnection()
        throws DataCenterException;
}
