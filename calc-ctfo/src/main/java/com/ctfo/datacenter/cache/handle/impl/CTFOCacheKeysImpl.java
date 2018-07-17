package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheKeys;
import com.ctfo.datacenter.cache.util.RedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Collection;
import java.util.List;

public class CTFOCacheKeysImpl
    implements CTFOCacheKeys {
    private Jedis[] arrayJedis;
    private int count = 10000;
    private ScanParams scanParams;
    private String cursor = "0";
    private int currJedis = 0;
    private List<String> list;

    public CTFOCacheKeysImpl(ShardControl shardControl, String dbname, String tablename) {
        Collection<Jedis> colJedis = shardControl.getConnection().getAllShards();
        this.arrayJedis = ((Jedis[]) colJedis.toArray(new Jedis[colJedis.size()]));
        this.scanParams = new ScanParams();
        String pattern = RedisUtil.getDataPatternKey(dbname, tablename);
        this.scanParams.match(pattern);
    }

    @Override
    public boolean next()
        throws DataCenterException {
        return next(this.count);
    }

    @Override
    public boolean next(int count)
        throws DataCenterException {
        this.scanParams.count(count);
        if (this.currJedis < this.arrayJedis.length) {
            ScanResult<String> scanResult = this.arrayJedis[this.currJedis].scan(this.cursor, this.scanParams);
            this.list = scanResult.getResult();
            this.cursor = scanResult.getStringCursor();
            if ("0".equals(this.cursor)) {
                this.currJedis += 1;
                if (this.list.size() == 0) {
                    return next(count);
                }
            }
            return true;
        }
        count = 10000;
        this.currJedis = 0;
        for (int i = 0; i < this.arrayJedis.length; i++) {
            if (this.arrayJedis[i] != null) {
                this.arrayJedis[i].close();
            }
        }
        return false;
    }

    @Override
    public List<String> getKeys()
        throws DataCenterException {
        return this.list;
    }
}
