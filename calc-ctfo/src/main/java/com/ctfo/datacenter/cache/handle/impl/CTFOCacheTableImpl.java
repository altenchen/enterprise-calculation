package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.impl.ShardControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheKeys;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import com.ctfo.datacenter.cache.handle.CTFOCacheTablePipeline;
import com.ctfo.datacenter.cache.message.PubSubManager;
import com.ctfo.datacenter.cache.util.RedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ShardedJedis;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CTFOCacheTableImpl
    implements CTFOCacheTable {
    private ConfigControl configControl;
    private String dbname;
    private String tablename;
    private int seconds = 0;
    private ShardControl shardControl;
    private ShardControl addShardControl;
    private String address;
    private ShardedJedis shardedJedis;
    private String addresskey;

    protected CTFOCacheTableImpl() {
    }

    protected CTFOCacheTableImpl(ConfigControl configControl, ShardControl shardControl, ShardControl addShardControl, String dbname, String tablename, int seconds, String address)
        throws DataCenterException {
        this.configControl = configControl;
        this.shardControl = shardControl;
        this.addShardControl = addShardControl;
        this.dbname = dbname;
        this.tablename = tablename;
        this.seconds = seconds;
        this.address = address;
        this.addresskey = RedisUtil.getConfigKey("address");
    }

    @Override
    public CTFOCacheTablePipeline getPipeline() {
        CTFOCacheTablePipeline pipeline = new CTFOCacheTablePipelineImpl(this.shardControl, this.addShardControl, this.dbname, this.tablename, this.seconds);
        return pipeline;
    }

    private synchronized void shardStart()
        throws DataCenterException {
        if (this.addShardControl == null) {
            Map<String, String> map = this.configControl.queryHash(this.addresskey);
            this.addShardControl = new ShardControlImpl(map);
        }
    }

    private synchronized void shardEnd()
        throws DataCenterException {
        if (!"0".equals(PubSubManager.getShardStatus(this.address))) {
            if (this.addShardControl != null) {
                this.addShardControl.destoryPool();
                this.addShardControl = null;
                System.out.println("addShardControl = null");
            }
            Map<String, String> map = this.configControl.queryHash(this.addresskey);
            if (this.shardControl != null) {
                System.out.println("shardControl = null");
            }
            this.shardControl.reConnection(map);

            PubSubManager.setShardStatus(this.address, "0");
            System.out.println("PubSubManager.setShardStatus(address,Constant.SHARD_NORMAL_STATUS);");
        }
        System.out.println("no no no PubSubManager.setShardStatus(address,Constant.SHARD_NORMAL_STATUS);");
    }

    public void reConnection(String key) {
        if (this.shardedJedis != null) {
            ((Jedis) this.shardedJedis.getShard(key)).connect();
        }
    }

    public boolean isConnection(String key) {
        boolean result = false;
        if (this.shardedJedis != null) {
            result = ((Jedis) this.shardedJedis.getShard(key)).isConnected();
        }
        return result;
    }

    @Override
    public String add(String key, String value)
        throws DataCenterException {
        return Padd(key, this.seconds, value);
    }

    @Override
    public String add(String key, int seconds, String value)
        throws DataCenterException {
        return Padd(key, seconds, value);
    }

    private String Padd(String key, int seconds, String value)
        throws DataCenterException {
        String result = "0";
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.add(tablekey, seconds, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.add(tablekey, seconds, value);
            if ("OK".equals(result)) {
                result = "1";
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String add(String key, byte[] value)
        throws DataCenterException {
        return Badd(key, this.seconds, value);
    }

    @Override
    public String add(String key, int seconds, byte[] value)
        throws DataCenterException {
        return Badd(key, seconds, value);
    }

    private String Badd(String key, int seconds, byte[] value)
        throws DataCenterException {
        String result = "0";
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.add(byteKey, seconds, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.add(byteKey, seconds, value);
            if ("OK".equals(result)) {
                result = "1";
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long addRList(String key, List<String> values)
        throws DataCenterException {
        if (values != null) {
            return PaddRList(key, this.seconds, (String[]) values.toArray(new String[values.size()]));
        }
        throw new DataCenterException("param error");
    }

    @Override
    public Long addRList(String key, int seconds, List<String> values)
        throws DataCenterException {
        if (values != null) {
            return PaddRList(key, seconds, (String[]) values.toArray(new String[values.size()]));
        }
        throw new DataCenterException("param error");
    }

    @Override
    public Long addRList(String key, String... values)
        throws DataCenterException {
        return PaddRList(key, this.seconds, values);
    }

    @Override
    public Long addRList(String key, int seconds, String... values)
        throws DataCenterException {
        return PaddRList(key, seconds, values);
    }

    private Long PaddRList(String key, int seconds, String... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addRList(tablekey, seconds, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addRList(tablekey, seconds, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addRList(String key, byte[]... values)
        throws DataCenterException {
        return BaddRList(key, this.seconds, values);
    }

    @Override
    public Long addRList(String key, int seconds, byte[]... values)
        throws DataCenterException {
        return BaddRList(key, seconds, values);
    }

    private Long BaddRList(String key, int seconds, byte[]... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addRList(byteKey, seconds, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addRList(byteKey, seconds, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addLList(String key, List<String> values)
        throws DataCenterException {
        if (values != null) {
            return PaddLList(key, this.seconds, (String[]) values.toArray(new String[values.size()]));
        }
        throw new DataCenterException("param error");
    }

    @Override
    public Long addLList(String key, int seconds, List<String> values)
        throws DataCenterException {
        if (values != null) {
            return PaddLList(key, seconds, (String[]) values.toArray(new String[values.size()]));
        }
        throw new DataCenterException("param error");
    }

    @Override
    public Long addLList(String key, String... values)
        throws DataCenterException {
        return PaddLList(key, this.seconds, values);
    }

    @Override
    public Long addLList(String key, int seconds, String... values)
        throws DataCenterException {
        return PaddLList(key, seconds, values);
    }

    private Long PaddLList(String key, int seconds, String... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addLList(tablekey, seconds, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addLList(tablekey, seconds, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addLList(String key, byte[]... values)
        throws DataCenterException {
        return BaddLList(key, this.seconds, values);
    }

    @Override
    public Long addLList(String key, int seconds, byte[]... values)
        throws DataCenterException {
        return BaddLList(key, seconds, values);
    }

    private Long BaddLList(String key, int seconds, byte[]... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addLList(byteKey, seconds, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addLList(byteKey, seconds, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addSet(String key, String... values)
        throws DataCenterException {
        return PaddSet(key, this.seconds, values);
    }

    @Override
    public Long addSet(String key, int seconds, String... values)
        throws DataCenterException {
        return PaddSet(key, seconds, values);
    }

    private Long PaddSet(String key, int seconds, String... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addSet(tablekey, seconds, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addSet(tablekey, seconds, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addSet(String key, byte[]... values)
        throws DataCenterException {
        return BaddSet(key, this.seconds, values);
    }

    @Override
    public Long addSet(String key, int seconds, byte[]... values)
        throws DataCenterException {
        return BaddSet(key, seconds, values);
    }

    private Long BaddSet(String key, int seconds, byte[]... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addSet(byteKey, seconds, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addSet(byteKey, seconds, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addSortedSet(String key, double score, String value)
        throws DataCenterException {
        return PaddSortedSet(key, this.seconds, score, value);
    }

    @Override
    public Long addSortedSet(String key, int seconds, double score, String value)
        throws DataCenterException {
        return PaddSortedSet(key, seconds, score, value);
    }

    private Long PaddSortedSet(String key, int seconds, double score, String value)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addSortedSet(tablekey, seconds, score, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addSortedSet(tablekey, seconds, score, value).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addSortedSet(String key, double score, byte[] value)
        throws DataCenterException {
        return BaddSortedSet(key, this.seconds, score, value);
    }

    @Override
    public Long addSortedSet(String key, int seconds, double score, byte[] value)
        throws DataCenterException {
        return BaddSortedSet(key, seconds, score, value);
    }

    private Long BaddSortedSet(String key, int seconds, double score, byte[] value)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addSortedSet(byteKey, seconds, score, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addSortedSet(byteKey, seconds, score, value).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addHash(String key, String field, String value)
        throws DataCenterException {
        return PaddHash(key, this.seconds, field, value);
    }

    @Override
    public Long addHash(String key, int seconds, String field, String value)
        throws DataCenterException {
        return PaddHash(key, seconds, field, value);
    }

    private Long PaddHash(String key, int seconds, String field, String value)
        throws DataCenterException {
        long result = 0L;
        if ((key != null) && (field != null) && (value != null) && (key.length() > 0) && (field.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addHash(tablekey, seconds, field, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addHash(tablekey, seconds, field, value).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long addHash(String key, byte[] field, byte[] value)
        throws DataCenterException {
        return BaddHash(key, this.seconds, field, value);
    }

    @Override
    public Long addHash(String key, int seconds, byte[] field, byte[] value)
        throws DataCenterException {
        return BaddHash(key, seconds, field, value);
    }

    private Long BaddHash(String key, int seconds, byte[] field, byte[] value)
        throws DataCenterException {
        long result = 0L;
        if ((key != null) && (field != null) && (value != null) && (key.length() > 0) && (field.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addHash(byteKey, seconds, field, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addHash(byteKey, seconds, field, value).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public String addHash(String key, Map<String, String> values)
        throws DataCenterException {
        return PaddHash(key, this.seconds, values);
    }

    @Override
    public String addHash(String key, int seconds, Map<String, String> values)
        throws DataCenterException {
        return PaddHash(key, seconds, values);
    }

    private String PaddHash(String key, int seconds, Map<String, String> values)
        throws DataCenterException {
        String result = "0";
        if ((key != null) && (values != null) && (key.length() > 0) && (values.size() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addHash(tablekey, seconds, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addHash(tablekey, seconds, values);
            if ("OK".equals(result)) {
                result = "1";
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String addHashByte(String key, Map<byte[], byte[]> values)
        throws DataCenterException {
        return BaddHash(key, this.seconds, values);
    }

    @Override
    public String addHashByte(String key, int seconds, Map<byte[], byte[]> values)
        throws DataCenterException {
        return BaddHash(key, seconds, values);
    }

    private String BaddHash(String key, int seconds, Map<byte[], byte[]> values)
        throws DataCenterException {
        String result = "0";
        if ((key != null) && (values != null) && (key.length() > 0) && (values.size() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.addHash(byteKey, seconds, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.addHash(byteKey, seconds, values);
            if ("OK".equals(result)) {
                result = "1";
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long delete(String key)
        throws DataCenterException {
        Long result = Long.valueOf(-1L);
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.delete(tablekey);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.delete(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long deleteHashField(String key, String... fields)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (fields != null) && (key.length() > 0) && (fields.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteHashField(tablekey, fields);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteHashField(tablekey, fields).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteHashField(String key, byte[]... fields)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (fields != null) && (key.length() > 0) && (fields.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteHashField(byteKey, fields);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteHashField(byteKey, fields).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteSetValue(String key, String... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteSetValue(tablekey, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteSetValue(tablekey, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteSetValue(String key, byte[]... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteSetValue(byteKey, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteSetValue(byteKey, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteSortedSetValue(String key, String... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteSortedSetValue(tablekey, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteSortedSetValue(tablekey, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteSortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteSortedSetByScore(tablekey, start, end);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteSortedSetByScore(tablekey, start, end).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteSortedSetByRank(String key, long start, long end)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteSortedSetByRank(tablekey, start, end);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteSortedSetByRank(tablekey, start, end).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteSortedSetValue(String key, byte[]... values)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteSortedSetValue(byteKey, values);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteSortedSetValue(byteKey, values).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteListValue(String key, long count, String value)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteListValue(tablekey, count, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteListValue(tablekey, count, value).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long deleteListValue(String key, int count, byte[] value)
        throws DataCenterException {
        long result = -1L;
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.deleteListValue(byteKey, count, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.deleteListValue(byteKey, count, value).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Long queryTTL(String key)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryTTL(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String query(String key)
        throws DataCenterException {
        String result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.query(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public byte[] queryByte(String key)
        throws DataCenterException {
        byte[] result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.query(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String queryListIndexValue(String key, Long index)
        throws DataCenterException {
        String result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryListIndexValue(tablekey, index);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public List<String> queryList(String key)
        throws DataCenterException {
        List<String> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryList(tablekey, Long.valueOf(0L), Long.valueOf(-1L));
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public List<byte[]> queryListByte(String key)
        throws DataCenterException {
        List<byte[]> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.queryList(byteKey, 0, -1);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String queryRList(String key)
        throws DataCenterException {
        List<String> list = null;
        String result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            list = this.shardControl.queryList(tablekey, Long.valueOf(-1L), Long.valueOf(-1L));
            if (list.size() > 0) {
                result = (String) list.get(0);
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public byte[] queryRListByte(String key)
        throws DataCenterException {
        List<byte[]> list = null;
        byte[] result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            list = this.shardControl.queryList(byteKey, -1, -1);
            if (list.size() > 0) {
                result = (byte[]) list.get(0);
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String queryLList(String key)
        throws DataCenterException {
        List<String> list = null;
        String result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            list = this.shardControl.queryList(tablekey, Long.valueOf(0L), Long.valueOf(0L));
            if (list.size() > 0) {
                result = (String) list.get(0);
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public byte[] queryLListByte(String key)
        throws DataCenterException {
        List<byte[]> list = null;
        byte[] result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            list = this.shardControl.queryList(byteKey, 0, 0);
            if (list.size() > 0) {
                result = (byte[]) list.get(0);
            }
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String popRList(String key)
        throws DataCenterException {
        String result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.popRList(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public byte[] popRListByte(String key)
        throws DataCenterException {
        byte[] result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.popRList(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String popLList(String key)
        throws DataCenterException {
        String result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.popLList(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public byte[] popLListByte(String key)
        throws DataCenterException {
        byte[] result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.popLList(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long queryListSize(String key)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryListSize(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Set<String> querySet(String key)
        throws DataCenterException {
        Set<String> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.querySet(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Set<byte[]> querySetByte(String key)
        throws DataCenterException {
        Set<byte[]> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.querySet(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long querySetSize(String key)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.querySetSize(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Set<String> querySortedSet(String key)
        throws DataCenterException {
        Set<String> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.querySortedSet(tablekey, 0L, -1L);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Set<String> querySortedSet(String key, long start, long end)
        throws DataCenterException {
        Set<String> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.querySortedSet(tablekey, start, end);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Set<String> querySortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        Set<String> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.querySortedSetByScore(tablekey, start, end);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Set<byte[]> querySortedSetByte(String key)
        throws DataCenterException {
        Set<byte[]> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.querySortedSet(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long querySortedSetSize(String key)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.querySortedSetSize(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Map<String, String> queryHash(String key)
        throws DataCenterException {
        Map<String, String> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryHash(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Map<byte[], byte[]> queryHashByte(String key)
        throws DataCenterException {
        Map<byte[], byte[]> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.queryHash(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Set<String> queryHashField(String key)
        throws DataCenterException {
        Set<String> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryHashField(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Set<byte[]> queryHashFieldByte(String key)
        throws DataCenterException {
        Set<byte[]> result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.queryHashField(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public String queryHash(String key, String field)
        throws DataCenterException {
        String result = null;
        if ((key != null) && (field != null) && (key.length() > 0) && (field.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryHash(tablekey, field);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public byte[] queryHashByte(String key, byte[] field)
        throws DataCenterException {
        byte[] result = null;
        if ((key != null) && (field != null) && (key.length() > 0) && (field.length > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.queryHash(byteKey, field);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public List<String> queryHash(String key, String... fields)
        throws DataCenterException {
        List<String> result = null;
        if ((key != null) && (fields != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryHash(tablekey, fields);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public List<byte[]> queryHash(String key, byte[]... fields)
        throws DataCenterException {
        List<byte[]> result = null;
        if ((key != null) && (fields != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            result = this.shardControl.queryHash(byteKey, fields);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long queryHashSize(String key)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.queryHashSize(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long incr(String key)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.incr(tablekey);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.incr(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long incr(String key, int value)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.incr(tablekey, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.incr(tablekey, value);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long decr(String key)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.decr(tablekey);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.decr(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long decr(String key, int value)
        throws DataCenterException {
        Long result = null;
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.decr(tablekey, value);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.decr(tablekey, value);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Long expire(String key, int seconds)
        throws DataCenterException {
        long result = 0L;
        if ((key != null) && (seconds > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if ("1".equals(PubSubManager.getShardStatus(this.address))) {
                shardStart();
                this.addShardControl.expire(tablekey, seconds);
            } else if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            result = this.shardControl.expire(tablekey, seconds).longValue();
        } else {
            throw new DataCenterException("param error");
        }
        return Long.valueOf(result);
    }

    @Override
    public Boolean isSetValueExist(String key, String value)
        throws DataCenterException {
        Boolean result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.isSetValueExist(tablekey, value);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public Boolean isKeyExist(String key)
        throws DataCenterException {
        Boolean result = null;
        if ((key != null) && (key.length() > 0)) {
            if (("6".equals(PubSubManager.getShardStatus(this.address))) || ("R".equals(PubSubManager.getShardStatus(this.address)))) {
                shardEnd();
            }
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            result = this.shardControl.isKeyExist(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
        return result;
    }

    @Override
    public CTFOCacheKeys getCTFOCacheKeys()
        throws DataCenterException {
        CTFOCacheKeys cacheKeys = new CTFOCacheKeysImpl(this.shardControl, this.dbname, this.tablename);
        return cacheKeys;
    }

    @Override
    public long clear()
        throws DataCenterException {
        long result = 0L;
        String pattern = RedisUtil.getDataPatternKey(this.dbname, this.tablename);
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
        return result;
    }
}
