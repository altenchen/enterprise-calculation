package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.List;

public abstract interface CTFOCacheKeys {
    public abstract boolean next()
        throws DataCenterException;

    public abstract boolean next(int paramInt)
        throws DataCenterException;

    public abstract List<String> getKeys()
        throws DataCenterException;
}
