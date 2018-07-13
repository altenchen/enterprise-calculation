package com.ctfo.datacenter.cache.handle.base;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.Set;

public abstract interface KeysControl {
    public abstract Set<String> getkey(long paramLong)
        throws DataCenterException;

    public abstract String getkey()
        throws DataCenterException;
}
