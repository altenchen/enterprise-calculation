package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheThriftDB;

public class CTFOCacheThriftDBImpl
    extends CTFOCacheDBImpl
    implements CTFOCacheThriftDB {
    public CTFOCacheThriftDBImpl(String dbname, String redisAddr) {
        super(dbname, redisAddr);
    }

    public CTFOCacheThriftDBImpl(ConfigControl configControl, ShardControl shardControl, ShardControl addShardControl, String dbname, String address) {
        super(configControl, shardControl, addShardControl, dbname, address);
    }

    @Override
    public long simpleGetTable(String tablename)
        throws DataCenterException {
        return this.configControl.addHash(this.dbname_key, tablename, "0").longValue();
    }

    @Override
    public long simpleGetTable(String tablename, int seconds)
        throws DataCenterException {
        return this.configControl.addHash(this.dbname_key, tablename, "0").longValue();
    }
}
