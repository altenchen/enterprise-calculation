package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ConnConfig;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOMonitorDBManager;
import com.ctfo.datacenter.cache.util.RedisUtil;

import java.util.List;
import java.util.Map;

public class CTFOMonitorDBManagerImpl
    extends CTFODBManagerImpl
    implements CTFOMonitorDBManager {
    public CTFOMonitorDBManagerImpl(String address, ConnConfig connConfig)
        throws DataCenterException {
        super(address, connConfig);
    }

    @Override
    public List<String> getAllMonitor()
        throws DataCenterException {
        String monitorkey = RedisUtil.getConfigKey("mon", "address");
        List<String> addrlist = this.configControl.queryList(monitorkey, Long.valueOf(0L), Long.valueOf(-1L));
        return addrlist;
    }

    @Override
    public Map<String, String> getMonitorData(String addr)
        throws DataCenterException {
        String monitorkey = RedisUtil.getConfigKey("mon", addr);
        Map<String, String> map = this.configControl.queryHash(monitorkey);
        return map;
    }

    @Override
    public String modifyMonitorData(String addr, Map<String, String> map)
        throws DataCenterException {
        String monitorkey = RedisUtil.getConfigKey("mon", addr);
        String s = this.configControl.addHash(monitorkey, map);
        return s;
    }
}
