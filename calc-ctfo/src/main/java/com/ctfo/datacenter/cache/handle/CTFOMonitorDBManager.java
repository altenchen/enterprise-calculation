package com.ctfo.datacenter.cache.handle;

import com.ctfo.datacenter.cache.exception.DataCenterException;

import java.util.List;
import java.util.Map;

public abstract interface CTFOMonitorDBManager
    extends CTFODBManager {
    public abstract List<String> getAllMonitor()
        throws DataCenterException;

    public abstract Map<String, String> getMonitorData(String paramString)
        throws DataCenterException;

    public abstract String modifyMonitorData(String paramString, Map<String, String> paramMap)
        throws DataCenterException;
}
