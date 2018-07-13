package com.ctfo.datacenter.cache.conn.impl;

import com.ctfo.datacenter.cache.conn.ConnConfig;
import com.ctfo.datacenter.cache.conn.ConnectionPoolFactory;
import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.ShardPipelineControl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import redis.clients.jedis.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShardControlImpl
    implements ShardControl {
    private ShardedJedisPool shardedJedisPool = null;

    public ShardControlImpl(Map<String, String> map, ConnConfig connConfig) {
        initJedisPool(map, connConfig);
    }

    public ShardControlImpl(Map<String, String> map) {
        ConnConfig connConfig = new ConnConfig();
        initJedisPool(map, connConfig);
    }

    @Override
    public void reConnection(Map<String, String> map) {
        ConnConfig connConfig = new ConnConfig();
        initJedisPool(map, connConfig);
    }

    @Override
    public void initJedisPool(Map<String, String> map, ConnConfig connConfig) {
        this.shardedJedisPool = ConnectionPoolFactory.getShardedJedisPool(map, connConfig);
    }

    @Override
    public ShardedJedis getConnection() {
        return (ShardedJedis) this.shardedJedisPool.getResource();
    }

    @Override
    public void freeConnection(ShardedJedis shardedJedis) {
        if (shardedJedis != null) {
            this.shardedJedisPool.returnResource(shardedJedis);
        }
    }

    @Override
    public void brokenConnection(ShardedJedis shardedJedis) {
        this.shardedJedisPool.returnBrokenResource(shardedJedis);
    }

    @Override
    public void destoryPool() {
        if (this.shardedJedisPool != null) {
            this.shardedJedisPool.destroy();
        }
    }

    @Override
    public String add(String key, int seconds, String value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.set(key, value);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-add-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String add(byte[] key, int seconds, byte[] value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.set(key, value);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-add-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addRList(String key, int seconds, String... values)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.rpush(key, values);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addRList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addRList(byte[] key, int seconds, byte[]... values)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.rpush(key, values);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addRList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addLList(String key, int seconds, String... values)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.lpush(key, values);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addLList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addLList(byte[] key, int seconds, byte[]... values)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.lpush(key, values);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addLList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addSet(String key, int seconds, String... values)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.sadd(key, values);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addSet(byte[] key, int seconds, byte[]... values)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.sadd(key, values);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addSortedSet(String key, int seconds, double score, String value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.zadd(key, score, value);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addSortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addSortedSet(byte[] key, int seconds, double score, byte[] value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.zadd(key, score, value);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addSortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addHash(String key, int seconds, String field, String value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hset(key, field, value);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long addHash(byte[] key, int seconds, byte[] field, byte[] value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hset(key, field, value);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String addHash(String key, int seconds, Map<String, String> values)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hmset(key, values);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String addHash(byte[] key, int seconds, Map<byte[], byte[]> values)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hmset(key, values);
            if (seconds > 0) {
                shardedJedis.expire(key, seconds);
            }
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-addHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long delete(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.del(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-delete-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long deleteHashField(String key, String... fields)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.hdel(key, fields);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteHashField-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteHashField(byte[] key, byte[]... fields)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.hdel(key, fields);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteHashField-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteSetValue(String key, String... values)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.srem(key, values);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteSetValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteSetValue(byte[] key, byte[]... values)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.srem(key, values);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteSetValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteSortedSetByRank(String key, long start, long end)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteSortedSetByScore-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteSortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteSortedSetByScore-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteSortedSetValue(String key, String... values)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.zrem(key, values);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteSortedSetValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteSortedSetValue(byte[] key, byte[]... values)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.zrem(key, values);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteSortedSetValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteListValue(String key, long count, String value)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.lrem(key, count, value);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteListValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public Long deleteListValue(byte[] key, int count, byte[] value)
        throws DataCenterException {
        Long l = null;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getConnection();
            l = shardedJedis.lrem(key, count, value);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-deleteListValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return l;
    }

    @Override
    public String query(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.get(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-query-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public byte[] query(byte[] key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        byte[] result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.get(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-query-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String queryListIndexValue(String key, Long index)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.lindex(key, index.longValue());
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryListIndexValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public List<String> queryList(String key, Long start, Long end)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        List<String> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.lrange(key, start.longValue(), end.longValue());
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public List<byte[]> queryList(byte[] key, int start, int end)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        List<byte[]> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.lrange(key, start, end);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String popRList(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.rpop(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-popRList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public byte[] popRList(byte[] key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        byte[] result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.rpop(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-popRList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String popLList(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.lpop(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-popLList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public byte[] popLList(byte[] key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        byte[] result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.lpop(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-popLList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long queryListSize(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.llen(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryListSize-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Set<String> querySet(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Set<String> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.smembers(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-querySet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Set<byte[]> querySet(byte[] key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Set<byte[]> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.smembers(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-querySet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long querySetSize(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.scard(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-querySetSize-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Set<String> querySortedSet(String key, long start, long end)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Set<String> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.zrange(key, start, end);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-querySortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Set<String> querySortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Set<String> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.zrangeByScore(key, start, end);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-querySortedSetByScore-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Set<byte[]> querySortedSet(byte[] key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Set<byte[]> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.zrange(key, 0L, -1L);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-querySortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long querySortedSetSize(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.zcard(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-querySortedSetSize-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Set<Tuple> queryObjectSortedSet(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Set<Tuple> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.zrevrangeWithScores(key, 0L, -1L);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryObjectSortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Map<String, String> queryHash(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Map<String, String> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hgetAll(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Map<byte[], byte[]> queryHash(byte[] key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Map<byte[], byte[]> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hgetAll(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Set<String> queryHashField(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Set<String> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hkeys(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHashField-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Set<byte[]> queryHashField(byte[] key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Set<byte[]> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hkeys(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHashField-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String queryHash(String key, String field)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hget(key, field);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public byte[] queryHash(byte[] key, byte[] field)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        byte[] result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hget(key, field);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public List<String> queryHash(String key, String... fields)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        List<String> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hmget(key, fields);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public List<byte[]> queryHash(byte[] key, byte[]... fields)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        List<byte[]> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hmget(key, fields);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long queryHashSize(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.hlen(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryHashSize-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long queryTTL(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.ttl(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryTTL-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String queryType(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.type(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryType-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Collection<Jedis> queryAllShards()
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Collection<Jedis> result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.getAllShards();
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[]ShardControl-queryAllShards-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public String queryAddress(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getConnection();
            Client client = ((Jedis) shardedJedis.getShard(key)).getClient();
            result = client.getHost() + ":" + client.getPort();
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-queryAddress-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long incr(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.incr(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-incr-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long incr(String key, int value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.incrBy(key, value);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-incr-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long decr(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.decr(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-decr-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long decr(String key, int value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.decrBy(key, value);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-decr-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Long expire(String key, int seconds)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Long result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.expire(key, seconds);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-expire-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Boolean isSetValueExist(String key, String value)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Boolean result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.sismember(key, value);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-isSetValueExist-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public Boolean isKeyExist(String key)
        throws DataCenterException {
        ShardedJedis shardedJedis = null;
        Boolean result = null;
        try {
            shardedJedis = getConnection();
            result = shardedJedis.exists(key);
        } catch (Exception e) {
            brokenConnection(shardedJedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardControl-isKeyExist-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(shardedJedis);
        }
        return result;
    }

    @Override
    public ShardPipelineControl getPipelineInstance()
        throws DataCenterException {
        try {
            return new ShardPipelineControlImpl(this.shardedJedisPool);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[]ShardControl-getPipelineInstance-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        }
    }
}
