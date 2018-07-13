package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheDB;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import com.ctfo.datacenter.cache.util.RedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class CTFOCacheDBImpl
    implements CTFOCacheDB {
    protected ConfigControl configControl;
    protected String dbname_key;
    private String dbname;
    private String address;
    private ShardControl addShardControl;
    private ShardControl shardControl;

    protected CTFOCacheDBImpl() {
    }

    public CTFOCacheDBImpl(String dbname, String redisAddr) {
        this.configControl = new ConfigControlImpl(redisAddr);
        this.dbname_key = RedisUtil.getConfigUserKey(dbname);
    }

    public CTFOCacheDBImpl(ConfigControl configControl, ShardControl shardControl, ShardControl addShardControl, String dbname, String address) {
        this.configControl = configControl;
        this.shardControl = shardControl;
        this.addShardControl = addShardControl;
        this.dbname = dbname;
        this.address = address;
        this.dbname_key = RedisUtil.getConfigUserKey(dbname);
    }

    @Override
    public CTFOCacheTable getTable(String tablename, int seconds)
        throws DataCenterException {
        if ((tablename != null) && (tablename.length() > 0) && (seconds > 0) && (tablename.indexOf("-") == -1)) {
            this.configControl.addHash(this.dbname_key, tablename, String.valueOf(seconds));
            CTFOCacheTable ctfoCacheData = new CTFOCacheTableImpl(this.configControl, this.shardControl, this.addShardControl, this.dbname, tablename, seconds, this.address);
            return ctfoCacheData;
        }
        throw new DataCenterException("param error");
    }

    @Override
    public CTFOCacheTable getTable(String tablename)
        throws DataCenterException {
        if ((tablename != null) && (tablename.length() > 0) && (tablename.indexOf("-") == -1)) {
            this.configControl.addHash(this.dbname_key, tablename, "0");
            CTFOCacheTable ctfoCacheData = new CTFOCacheTableImpl(this.configControl, this.shardControl, this.addShardControl, this.dbname, tablename, 0, this.address);
            return ctfoCacheData;
        }
        throw new DataCenterException("param error");
    }

    @Override
    public Boolean isTableExist(String tablename)
        throws DataCenterException {
        if ((tablename != null) && (tablename.length() > 0)) {
            Boolean b = this.configControl.isHashFieldExist(this.dbname_key, tablename);
            return b;
        }
        throw new DataCenterException("param error");
    }

    @Override
    public Set<String> getTableNames()
        throws DataCenterException {
        Set<String> set = this.configControl.queryHashField(this.dbname_key);
        return set;
    }

    @Override
    public long getTableSize()
        throws DataCenterException {
        long size = this.configControl.queryHashSize(this.dbname_key).longValue();
        return size;
    }

    @Override
    public long deleteTable(String tablename)
        throws DataCenterException {
        long result = 0L;
        if ((tablename != null) && (tablename.length() > 0)) {
            Boolean flag = this.configControl.isHashFieldExist(this.dbname_key, tablename);
            if (!flag.booleanValue()) {
                result = -1L;
                return result;
            }
            String pattern = RedisUtil.getDataPatternKey(this.dbname, tablename);
            ScanParams sp = new ScanParams();
            sp.count(10000);
            sp.match(pattern);

            Collection<Jedis> col = this.shardControl.getConnection().getAllShards();
            for (Jedis jedis : col) {
                String cursor = "0";
                for (; ; ) {
                    ScanResult<String> sr = jedis.scan(cursor, sp);
                    cursor = sr.getStringCursor();
                    List<String> list = sr.getResult();
                    int size = list.size();
                    if ((cursor == null) || ("0".equals(cursor))) {
                        if (size <= 0) {
                            break;
                        }
                        result += size;
                        jedis.del((String[]) list.toArray(new String[size]));
                        break;
                    }
                    if (size > 0) {
                        result += size;
                        jedis.del((String[]) list.toArray(new String[size]));
                    }
                }
            }
            this.configControl.deleteHashField(this.dbname_key, new String[]{tablename});
            return result;
        }
        throw new DataCenterException("param error");
    }

    public long addTable(String tablename)
        throws DataCenterException {
        long result = 1L;
        if ((tablename != null) && (tablename.length() > 0) && (tablename.indexOf("-") == -1)) {
            Boolean flag = this.configControl.isHashFieldExist(this.dbname_key, tablename);
            if (flag.booleanValue()) {
                result = 0L;
            } else {
                result = this.configControl.addHash(this.dbname_key, tablename, "0").longValue();
            }
            return result;
        }
        throw new DataCenterException("param error");
    }

    public long addTable(String tablename, int seconds)
        throws DataCenterException {
        long result = 1L;
        if ((tablename != null) && (tablename.length() > 0) && (seconds > 0) && (tablename.indexOf("-") == -1)) {
            Boolean flag = this.configControl.isHashFieldExist(this.dbname_key, tablename);
            if (flag.booleanValue()) {
                result = 0L;
                return result;
            }
            result = this.configControl.addHash(this.dbname_key, tablename, "0").longValue();
            return result;
        }
        throw new DataCenterException("param error");
    }
}
