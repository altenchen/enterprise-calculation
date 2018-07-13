package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.base.*;

public abstract interface CTFOCacheTable
    extends QueryCacheTable, AddTimeOutCacheTable, AddCacheTable, CounterCacheTable, DeleteCacheTable {
    public abstract CTFOCacheKeys getCTFOCacheKeys()
        throws DataCenterException;

    public abstract CTFOCacheTablePipeline getPipeline()
        throws DataCenterException;

    public abstract long clear()
        throws DataCenterException;
}
