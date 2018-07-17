package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.ShardPipelineControl;
import com.ctfo.datacenter.cache.conn.impl.ShardPipelineControlImpl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheTablePipeline;
import com.ctfo.datacenter.cache.util.RedisUtil;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public class CTFOCacheTablePipelineImpl
    implements CTFOCacheTablePipeline {
    private String dbname;
    private String tablename;
    private int seconds;
    private ShardPipelineControl shardPipelineControl;
    private ShardPipelineControl addShardPipelineControl;
    private boolean flag = false;

    public CTFOCacheTablePipelineImpl(ShardControl shardControl, ShardControl addShardControl, String dbname, String tablename, int seconds) {
        this.dbname = dbname;
        this.tablename = tablename;
        this.seconds = seconds;
        this.shardPipelineControl = new ShardPipelineControlImpl(shardControl);
        if (addShardControl == null) {
            this.addShardPipelineControl = null;
            this.flag = false;
        } else {
            this.addShardPipelineControl = new ShardPipelineControlImpl(addShardControl);
            this.flag = true;
        }
    }

    @Override
    public void commit()
        throws DataCenterException {
        if (this.flag) {
            this.addShardPipelineControl.sync();
            this.addShardPipelineControl.reconnectPipeline();
        }
        this.shardPipelineControl.sync();
        this.shardPipelineControl.reconnectPipeline();
    }

    @Override
    public List<Object> commitAndReturnAll()
        throws DataCenterException {
        if (this.flag) {
            this.addShardPipelineControl.sync();
            this.addShardPipelineControl.reconnectPipeline();
        }
        List<Object> ret = this.shardPipelineControl.syncAndReturnAll();
        this.shardPipelineControl.reconnectPipeline();
        return ret;
    }

    @Override
    public void close()
        throws DataCenterException {
        if (this.flag) {
            this.addShardPipelineControl.disconnect();
        }
        this.shardPipelineControl.disconnect();
    }

    @Override
    public void add(String key, String value)
        throws DataCenterException {
        Padd(key, this.seconds, value);
    }

    @Override
    public void add(String key, int seconds, String value)
        throws DataCenterException {
        Padd(key, seconds, value);
    }

    private void Padd(String key, int seconds, String value)
        throws DataCenterException {
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.add(tablekey, seconds, value);
            }
            this.shardPipelineControl.add(tablekey, seconds, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void add(String key, byte[] value)
        throws DataCenterException {
        Badd(key, this.seconds, value);
    }

    @Override
    public void add(String key, int seconds, byte[] value)
        throws DataCenterException {
        Badd(key, seconds, value);
    }

    private void Badd(String key, int seconds, byte[] value)
        throws DataCenterException {
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.add(byteKey, seconds, value);
            }
            this.shardPipelineControl.add(byteKey, seconds, value);
        }
    }

    @Override
    public void addRList(String key, List<String> values)
        throws DataCenterException {
        if (values != null) {
            PaddRList(key, this.seconds, (String[]) values.toArray(new String[values.size()]));
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addRList(String key, int seconds, List<String> values)
        throws DataCenterException {
        if (values != null) {
            PaddRList(key, seconds, (String[]) values.toArray(new String[values.size()]));
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addRList(String key, String... values)
        throws DataCenterException {
        PaddRList(key, this.seconds, values);
    }

    @Override
    public void addRList(String key, int seconds, String... values)
        throws DataCenterException {
        PaddRList(key, seconds, values);
    }

    private void PaddRList(String key, int seconds, String... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.addRList(tablekey, seconds, values);
            }
            this.shardPipelineControl.addRList(tablekey, seconds, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addRList(String key, byte[]... values)
        throws DataCenterException {
        BaddRList(key, this.seconds, values);
    }

    @Override
    public void addRList(String key, int seconds, byte[]... values)
        throws DataCenterException {
        BaddRList(key, seconds, values);
    }

    private void BaddRList(String key, int seconds, byte[]... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.addRList(byteKey, seconds, values);
            }
            this.shardPipelineControl.addRList(byteKey, seconds, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addLList(String key, List<String> values)
        throws DataCenterException {
        if (values != null) {
            PaddLList(key, this.seconds, (String[]) values.toArray(new String[values.size()]));
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addLList(String key, int seconds, List<String> values)
        throws DataCenterException {
        if (values != null) {
            PaddLList(key, seconds, (String[]) values.toArray(new String[values.size()]));
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addLList(String key, String... values)
        throws DataCenterException {
        PaddLList(key, this.seconds, values);
    }

    @Override
    public void addLList(String key, int seconds, String... values)
        throws DataCenterException {
        PaddLList(key, seconds, values);
    }

    private void PaddLList(String key, int seconds, String... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.addLList(tablekey, seconds, values);
            }
            this.shardPipelineControl.addLList(tablekey, seconds, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addLList(String key, byte[]... values)
        throws DataCenterException {
        BaddLList(key, this.seconds, values);
    }

    @Override
    public void addLList(String key, int seconds, byte[]... values)
        throws DataCenterException {
        BaddLList(key, seconds, values);
    }

    private void BaddLList(String key, int seconds, byte[]... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.addLList(byteKey, seconds, values);
            }
            this.shardPipelineControl.addLList(byteKey, seconds, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addSet(String key, String... values)
        throws DataCenterException {
        PaddSet(key, this.seconds, values);
    }

    @Override
    public void addSet(String key, int seconds, String... values)
        throws DataCenterException {
        PaddSet(key, seconds, values);
    }

    private void PaddSet(String key, int seconds, String... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.addSet(tablekey, seconds, values);
            }
            this.shardPipelineControl.addSet(tablekey, seconds, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addSet(String key, byte[]... values)
        throws DataCenterException {
        BaddSet(key, this.seconds, values);
    }

    @Override
    public void addSet(String key, int seconds, byte[]... values)
        throws DataCenterException {
        BaddSet(key, seconds, values);
    }

    private void BaddSet(String key, int seconds, byte[]... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.addSet(byteKey, seconds, values);
            }
            this.shardPipelineControl.addSet(byteKey, seconds, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addSortedSet(String key, double score, String value)
        throws DataCenterException {
        PaddSortedSet(key, this.seconds, score, value);
    }

    @Override
    public void addSortedSet(String key, int seconds, double score, String value)
        throws DataCenterException {
        PaddSortedSet(key, seconds, score, value);
    }

    private void PaddSortedSet(String key, int seconds, double score, String value)
        throws DataCenterException {
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.addSortedSet(tablekey, seconds, score, value);
            }
            this.shardPipelineControl.addSortedSet(tablekey, seconds, score, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addSortedSet(String key, double score, byte[] value)
        throws DataCenterException {
        BaddSortedSet(key, this.seconds, score, value);
    }

    @Override
    public void addSortedSet(String key, int seconds, double score, byte[] value)
        throws DataCenterException {
        BaddSortedSet(key, seconds, score, value);
    }

    private void BaddSortedSet(String key, int seconds, double score, byte[] value)
        throws DataCenterException {
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.addSortedSet(byteKey, seconds, score, value);
            }
            this.shardPipelineControl.addSortedSet(byteKey, seconds, score, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addHash(String key, String field, String value)
        throws DataCenterException {
        PaddHash(key, this.seconds, field, value);
    }

    @Override
    public void addHash(String key, int seconds, String field, String value)
        throws DataCenterException {
        PaddHash(key, seconds, field, value);
    }

    private void PaddHash(String key, int seconds, String field, String value)
        throws DataCenterException {
        if ((key != null) && (field != null) && (value != null) && (key.length() > 0) && (field.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.addHash(tablekey, seconds, field, value);
            }
            this.shardPipelineControl.addHash(tablekey, seconds, field, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addHash(String key, byte[] field, byte[] value)
        throws DataCenterException {
        BaddHash(key, this.seconds, field, value);
    }

    @Override
    public void addHash(String key, int seconds, byte[] field, byte[] value)
        throws DataCenterException {
        BaddHash(key, seconds, field, value);
    }

    private void BaddHash(String key, int seconds, byte[] field, byte[] value)
        throws DataCenterException {
        if ((key != null) && (field != null) && (value != null) && (key.length() > 0) && (field.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.addHash(byteKey, seconds, field, value);
            }
            this.shardPipelineControl.addHash(byteKey, seconds, field, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addHash(String key, Map<String, String> values)
        throws DataCenterException {
        PaddHash(key, this.seconds, values);
    }

    @Override
    public void addHash(String key, int seconds, Map<String, String> values)
        throws DataCenterException {
        PaddHash(key, seconds, values);
    }

    private void PaddHash(String key, int seconds, Map<String, String> values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.size() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.addHash(tablekey, seconds, values);
            }
            this.shardPipelineControl.addHash(tablekey, seconds, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void addHashByte(String key, Map<byte[], byte[]> values)
        throws DataCenterException {
        BaddHash(key, this.seconds, values);
    }

    @Override
    public void addHashByte(String key, int seconds, Map<byte[], byte[]> values)
        throws DataCenterException {
        BaddHash(key, seconds, values);
    }

    private void BaddHash(String key, int seconds, Map<byte[], byte[]> values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.size() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.addHash(byteKey, seconds, values);
            }
            this.shardPipelineControl.addHash(byteKey, seconds, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void delete(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.delete(tablekey);
            }
            this.shardPipelineControl.delete(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteHashField(String key, String... fields)
        throws DataCenterException {
        if ((key != null) && (fields != null) && (key.length() > 0) && (fields.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.deleteHashField(tablekey, fields);
            }
            this.shardPipelineControl.deleteHashField(tablekey, fields);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteHashField(String key, byte[]... fields)
        throws DataCenterException {
        if ((key != null) && (fields != null) && (key.length() > 0) && (fields.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.deleteHashField(byteKey, fields);
            }
            this.shardPipelineControl.deleteHashField(byteKey, fields);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteSetValue(String key, String... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.deleteSetValue(tablekey, values);
            }
            this.shardPipelineControl.deleteSetValue(tablekey, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteSetValue(String key, byte[]... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.deleteSetValue(byteKey, values);
            }
            this.shardPipelineControl.deleteSetValue(byteKey, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteSortedSetValue(String key, String... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.deleteSortedSetValue(tablekey, values);
            }
            this.shardPipelineControl.deleteSortedSetValue(tablekey, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteSortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.deleteSortedSetByScore(tablekey, start, end);
            }
            this.shardPipelineControl.deleteSortedSetByScore(tablekey, start, end);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteSortedSetByRank(String key, long start, long end)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.deleteSortedSetByRank(tablekey, start, end);
            }
            this.shardPipelineControl.deleteSortedSetByRank(tablekey, start, end);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteSortedSetValue(String key, byte[]... values)
        throws DataCenterException {
        if ((key != null) && (values != null) && (key.length() > 0) && (values.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.deleteSortedSetValue(byteKey, values);
            }
            this.shardPipelineControl.deleteSortedSetValue(byteKey, values);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteListValue(String key, long count, String value)
        throws DataCenterException {
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.deleteListValue(tablekey, count, value);
            }
            this.shardPipelineControl.deleteListValue(tablekey, count, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void deleteListValue(String key, int count, byte[] value)
        throws DataCenterException {
        if ((key != null) && (value != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            if (this.flag) {
                this.addShardPipelineControl.deleteListValue(byteKey, count, value);
            }
            this.shardPipelineControl.deleteListValue(byteKey, count, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryTTL(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryTTL(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void query(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.query(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.query(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryList(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryList(tablekey, Long.valueOf(0L), Long.valueOf(-1L));
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryListByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.queryList(byteKey, 0, -1);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryRList(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryList(tablekey, Long.valueOf(-1L), Long.valueOf(-1L));
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryRListByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.queryList(byteKey, -1, -1);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryLList(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryList(tablekey, Long.valueOf(0L), Long.valueOf(0L));
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryLListByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.queryList(byteKey, 0, 0);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void popRList(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.popRList(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void popRListByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.popRList(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void popLList(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.popLList(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void popLListByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.popLList(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryListSize(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryListSize(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void querySet(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.querySet(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void querySetByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.querySet(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void querySetSize(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.querySetSize(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void querySortedSet(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.querySortedSet(tablekey, 0L, -1L);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void querySortedSet(String key, long start, long end)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.querySortedSet(tablekey, start, end);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void querySortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.querySortedSetByScore(tablekey, start, end);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void querySortedSetByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.querySortedSet(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void querySortedSetSize(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.querySortedSetSize(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHash(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryHash(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHashByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.queryHash(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHashField(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryHashField(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHashFieldByte(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.queryHashField(byteKey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHash(String key, String field)
        throws DataCenterException {
        if ((key != null) && (field != null) && (key.length() > 0) && (field.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryHash(tablekey, field);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHashByte(String key, byte[] field)
        throws DataCenterException {
        if ((key != null) && (field != null) && (key.length() > 0) && (field.length > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.queryHash(byteKey, field);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHash(String key, String... fields)
        throws DataCenterException {
        if ((key != null) && (fields != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryHash(tablekey, fields);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHash(String key, byte[]... fields)
        throws DataCenterException {
        if ((key != null) && (fields != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            byte[] byteKey;
            try {
                byteKey = tablekey.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new DataCenterException(e.getMessage());
            }
            this.shardPipelineControl.queryHash(byteKey, fields);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void queryHashSize(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.queryHashSize(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void incr(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.incr(tablekey);
            }
            this.shardPipelineControl.incr(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void incr(String key, int value)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.incr(tablekey, value);
            }
            this.shardPipelineControl.incr(tablekey, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void decr(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.decr(tablekey);
            }
            this.shardPipelineControl.decr(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void decr(String key, int value)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.decr(tablekey, value);
            }
            this.shardPipelineControl.decr(tablekey, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void expire(String key, int seconds)
        throws DataCenterException {
        if ((key != null) && (seconds > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            if (this.flag) {
                this.addShardPipelineControl.expire(tablekey, seconds);
            }
            this.shardPipelineControl.expire(tablekey, seconds);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void isSetValueExist(String key, String value)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.isSetValueExist(tablekey, value);
        } else {
            throw new DataCenterException("param error");
        }
    }

    @Override
    public void isKeyExist(String key)
        throws DataCenterException {
        if ((key != null) && (key.length() > 0)) {
            String tablekey = RedisUtil.getKey(this.dbname, this.tablename, key);
            this.shardPipelineControl.isKeyExist(tablekey);
        } else {
            throw new DataCenterException("param error");
        }
    }
}
