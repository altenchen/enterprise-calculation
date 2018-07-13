package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.ConnConfig;
import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.impl.ConfigControlImpl;
import com.ctfo.datacenter.cache.conn.impl.ShardControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheDB;
import com.ctfo.datacenter.cache.handle.CTFODBManager;
import com.ctfo.datacenter.cache.message.PubSubManager;
import com.ctfo.datacenter.cache.util.RedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CTFODBManagerImpl
    implements CTFODBManager {
    protected ConfigControl configControl;
    protected String config_db_key;
    protected String address;
    private ShardControl addShardControl;
    private ShardControl shardControl;
    private ConnConfig connConfig;

    protected CTFODBManagerImpl() {
    }

    public CTFODBManagerImpl(String address, ConnConfig connConfig)
        throws DataCenterException {
        this.address = address;
        this.connConfig = connConfig;
        initConnection();
        initShardConnection();
    }

    private void initConnection()
        throws DataCenterException {
        this.configControl = new ConfigControlImpl(this.address);
        PubSubManager.initShardStatus(this.address, new String[]{"shardchannel"});
        this.config_db_key = RedisUtil.getConfigKey("db");
    }

    private void initShardConnection()
        throws DataCenterException {
        String[] connstr = this.address.split(":");
        String key = null;
        String oldkey = null;
        if (connstr.length > 2) {
            key = RedisUtil.getConfigKey("address") + "-" + connstr[2];
            oldkey = RedisUtil.getConfigKey("oldaddress") + "-" + connstr[2];
        } else {
            key = RedisUtil.getConfigKey("address");
            oldkey = RedisUtil.getConfigKey("oldaddress");
        }
        if (!"0".equals(PubSubManager.getShardStatus(this.address))) {
            Map<String, String> map = this.configControl.queryHash(key);
            Map<String, String> oldmap = this.configControl.queryHash(oldkey);

            this.addShardControl = new ShardControlImpl(map, this.connConfig);
            this.shardControl = new ShardControlImpl(oldmap, this.connConfig);
        } else {
            Map<String, String> map = this.configControl.queryHash(key);
            this.shardControl = new ShardControlImpl(map, this.connConfig);
            PubSubManager.setShardStatus(this.address, "0");
        }
    }

    @Override
    public CTFOCacheDB openCacheDB(String dbname)
        throws DataCenterException {
        if ((dbname != null) && (dbname.length() > 0) && (!"cfg".equals(dbname)) && (dbname.indexOf("-") == -1)) {
            this.configControl.addSet(this.config_db_key, new String[]{dbname});
            CTFOCacheDB ctfoCacheDB = new CTFOCacheDBImpl(this.configControl, this.shardControl, this.addShardControl, dbname, this.address);
            return ctfoCacheDB;
        }
        throw new DataCenterException("param error");
    }

    @Override
    public Boolean isDBExist(String dbname)
        throws DataCenterException {
        if ((dbname != null) && (dbname.length() > 0)) {
            Boolean b = this.configControl.isSetValueExist(this.config_db_key, dbname);
            return b;
        }
        throw new DataCenterException("param error");
    }

    @Override
    public Set<String> getDBNames()
        throws DataCenterException {
        Set<String> set = this.configControl.querySet(this.config_db_key);
        return set;
    }

    @Override
    public long getTableSize(String dbname)
        throws DataCenterException {
        long result = 0L;
        if ((dbname != null) && (dbname.length() > 0)) {
            if (this.configControl.isSetValueExist(this.config_db_key, dbname).booleanValue()) {
                Map<String, String> map = this.configControl.queryHash(RedisUtil.getConfigUserKey(dbname));
                result = map.size();
            } else {
                result = -1L;
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public long deleteDB(String dbname)
        throws DataCenterException {
        if ((dbname != null) && (dbname.length() > 0)) {
            long result = 0L;
            if (!isDBExist(dbname).booleanValue()) {
                result = -1L;
                return result;
            }
            String dbname_key = RedisUtil.getConfigUserKey(dbname);

            String pattern = RedisUtil.getDataPatternKey(dbname);
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
                jedis.close();
            }
            this.configControl.delete(dbname_key);
            this.configControl.deleteSetValue(this.config_db_key, new String[]{dbname});
            return result;
        }
        throw new DataCenterException("param error");
    }

    public long addDB(String dbname)
        throws DataCenterException {
        long result = 1L;
        boolean flag = this.configControl.isSetValueExist(this.config_db_key, dbname).booleanValue();
        if (flag) {
            result = 0L;
        } else {
            this.configControl.addSet(this.config_db_key, new String[]{dbname});
        }
        return result;
    }

    @Override
    public void closeConnection()
        throws DataCenterException {
        if (this.configControl != null) {
            this.configControl.destoryConnection();
        }
        if (this.shardControl != null) {
            this.shardControl.destoryPool();
        }
        if (this.addShardControl != null) {
            this.addShardControl.destoryPool();
        }
    }
}
