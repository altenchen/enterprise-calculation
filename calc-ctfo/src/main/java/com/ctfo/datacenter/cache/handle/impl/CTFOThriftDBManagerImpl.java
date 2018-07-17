package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ConnConfig;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOThriftDBManager;
import com.ctfo.datacenter.cache.util.RedisUtil;

public class CTFOThriftDBManagerImpl
    extends CTFODBManagerImpl
    implements CTFOThriftDBManager {
    public CTFOThriftDBManagerImpl(String address, ConnConfig connConfig)
        throws DataCenterException {
        super(address, connConfig);
    }

    @Override
    public void initParm() {
        this.configControl = new ConfigControlImpl(this.address);
        this.config_db_key = RedisUtil.getConfigKey("db");
    }

    @Override
    public long simpleOpenCacheDB(String dbname)
        throws DataCenterException {
        return this.configControl.addSet(this.config_db_key, new String[]{dbname}).longValue();
    }
}
