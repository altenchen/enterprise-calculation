package com.ctfo.datacenter.cache.conn.impl;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.conn.ConnectionPoolFactory;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigControlImpl
    implements ConfigControl {
    private JedisPool jedisPool = null;
    private String address;

    public ConfigControlImpl(String address) {
        initJedisPool(address);
    }

    @Override
    public void initJedisPool(String address) {
        this.address = address;
        this.jedisPool = ConnectionPoolFactory.getJedisPool(address);
    }

    @Override
    public Jedis getConnection() {
        return (Jedis) this.jedisPool.getResource();
    }

    @Override
    public void freeConnection(Jedis jedis) {
        if (jedis != null) {
            this.jedisPool.returnResource(jedis);
        }
    }

    @Override
    public void brokenConnection(Jedis jedis) {
        this.jedisPool.returnBrokenResource(jedis);
    }

    @Override
    public void destoryConnection() {
        if (this.jedisPool != null) {
            this.jedisPool.destroy();
        }
    }

    @Override
    public void reConnection() {
        destoryConnection();
        this.jedisPool = ConnectionPoolFactory.getJedisPool(this.address);
    }

    @Override
    public void reConnection(String address) {
        destoryConnection();
        this.jedisPool = ConnectionPoolFactory.getJedisPool(address);
    }

    @Override
    public String add(String key, String value)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.set(key, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-add-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public String add(byte[] key, byte[] value)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.set(key, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-add-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addRList(String key, String... values)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.rpush(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addRList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addRList(byte[] key, byte[]... values)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.rpush(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addRList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addLList(String key, String... values)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.lpush(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addLList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addLList(byte[] key, byte[]... values)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.lpush(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addLList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addSet(String key, String... values)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.sadd(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addSet(byte[] key, byte[]... values)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.sadd(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addSortedSet(String key, double score, String value)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.zadd(key, score, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addSortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addSortedSet(byte[] key, double score, byte[] value)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.zadd(key, score, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addSortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long addHash(String key, String field, String value)
        throws DataCenterException {
        Jedis jedis = null;
        Long l = null;
        try {
            jedis = getConnection();
            l = jedis.hset(key, field, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long addHash(byte[] key, byte[] field, byte[] value)
        throws DataCenterException {
        Jedis jedis = null;
        Long l = null;
        try {
            jedis = getConnection();
            l = jedis.hset(key, field, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public String addHash(String key, Map<String, String> values)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.hmset(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public String addHash(byte[] key, Map<byte[], byte[]> values)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.hmset(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-addHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Boolean isKeyExist(String key)
        throws DataCenterException {
        Boolean b = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            b = jedis.exists(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-isKeyExist-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return b;
    }

    @Override
    public Boolean isSetValueExist(String key, String value)
        throws DataCenterException {
        Boolean b = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            b = jedis.sismember(key, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-isSetValueExist-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return b;
    }

    @Override
    public Boolean isHashFieldExist(String key, String field)
        throws DataCenterException {
        Boolean b = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            b = jedis.hexists(key, field);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-isHashFieldExist-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return b;
    }

    @Override
    public Long delete(String key)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.del(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-delete-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteListValue(String key, long count, String value)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.lrem(key, count, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteListValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteListValue(byte[] key, int count, byte[] value)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.lrem(key, count, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteListValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteSetValue(String key, String... values)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.srem(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteSetValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteSetValue(byte[] key, byte[]... values)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.srem(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteSetValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteHashField(String key, String... values)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.hdel(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteHashField-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteHashField(byte[] key, byte[]... values)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.hdel(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteHashField-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteSortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteSortedSetByScore-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteSortedSetValue(String key, String... values)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.zrem(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteSortedSetValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public Long deleteSortedSetValue(byte[] key, byte[]... values)
        throws DataCenterException {
        Long l = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            l = jedis.zrem(key, values);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteSortedSetValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return l;
    }

    @Override
    public String queryType(String key)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.type(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryType-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long queryTTL(String key)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.ttl(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryTTL-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public String query(String key)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.get(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-query-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public byte[] query(byte[] key)
        throws DataCenterException {
        Jedis jedis = null;
        byte[] result = null;
        try {
            jedis = getConnection();
            result = jedis.get(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-query-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public String queryListIndexValue(String key, Long index)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.lindex(key, index.longValue());
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryListIndexValue-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public List<String> queryList(String key, Long start, Long end)
        throws DataCenterException {
        Jedis jedis = null;
        List<String> result = null;
        try {
            jedis = getConnection();
            result = jedis.lrange(key, start.longValue(), end.longValue());
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public List<byte[]> queryList(byte[] key, int start, int end)
        throws DataCenterException {
        Jedis jedis = null;
        List<byte[]> result = null;
        try {
            jedis = getConnection();
            result = jedis.lrange(key, start, end);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long queryListSize(String key)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.llen(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryListSize-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public String popRList(String key)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.rpop(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-popRList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public byte[] popRList(byte[] key)
        throws DataCenterException {
        Jedis jedis = null;
        byte[] result = null;
        try {
            jedis = getConnection();
            result = jedis.rpop(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-popRList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public String popLList(String key)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.lpop(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-popLList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public byte[] popLList(byte[] key)
        throws DataCenterException {
        Jedis jedis = null;
        byte[] result = null;
        try {
            jedis = getConnection();
            result = jedis.lpop(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-popLList-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Set<String> querySet(String key)
        throws DataCenterException {
        Set<String> set = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            set = jedis.smembers(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-querySet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return set;
    }

    @Override
    public Set<byte[]> querySet(byte[] key)
        throws DataCenterException {
        Set<byte[]> set = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            set = jedis.smembers(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-querySet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return set;
    }

    @Override
    public Long querySetSize(String key)
        throws DataCenterException {
        Long result = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            result = jedis.scard(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-querySetSize-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Set<String> querySortedSet(String key, long start, long end)
        throws DataCenterException {
        Jedis jedis = null;
        Set<String> result = null;
        try {
            jedis = getConnection();
            result = jedis.zrange(key, start, end);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-querySortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Set<String> querySortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        Jedis jedis = null;
        Set<String> result = null;
        try {
            jedis = getConnection();
            result = jedis.zrangeByScore(key, start, end);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-querySortedSetByScore-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Set<byte[]> querySortedSet(byte[] key)
        throws DataCenterException {
        Jedis jedis = null;
        Set<byte[]> result = null;
        try {
            jedis = getConnection();
            result = jedis.zrange(key, 0L, -1L);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-querySortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long querySortedSetSize(String key)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.zcard(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-querySortedSetSize-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Set<Tuple> queryObjectSortedSet(String key)
        throws DataCenterException {
        Jedis jedis = null;
        Set<Tuple> result = null;
        try {
            jedis = getConnection();
            result = jedis.zrevrangeWithScores(key, 0L, -1L);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryObjectSortedSet-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Map<String, String> queryHash(String key)
        throws DataCenterException {
        Jedis jedis = null;
        Map<String, String> result = null;
        try {
            jedis = getConnection();
            result = jedis.hgetAll(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Map<byte[], byte[]> queryHash(byte[] key)
        throws DataCenterException {
        Jedis jedis = null;
        Map<byte[], byte[]> result = null;
        try {
            jedis = getConnection();
            result = jedis.hgetAll(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public List<String> queryHashValues(String key)
        throws DataCenterException {
        Jedis jedis = null;
        List<String> result = null;
        try {
            jedis = getConnection();
            result = jedis.hvals(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHashValues-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public String queryHash(String key, String field)
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.hget(key, field);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public byte[] queryHash(byte[] key, byte[] field)
        throws DataCenterException {
        Jedis jedis = null;
        byte[] result = null;
        try {
            jedis = getConnection();
            result = jedis.hget(key, field);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public List<String> queryHash(String key, String... fields)
        throws DataCenterException {
        Jedis jedis = null;
        List<String> result = null;
        try {
            jedis = getConnection();
            result = jedis.hmget(key, fields);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public List<byte[]> queryHash(byte[] key, byte[]... fields)
        throws DataCenterException {
        Jedis jedis = null;
        List<byte[]> result = null;
        try {
            jedis = getConnection();
            result = jedis.hmget(key, fields);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHash-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Set<String> queryHashField(String key)
        throws DataCenterException {
        Set<String> set = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            set = jedis.hkeys(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHashField-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return set;
    }

    @Override
    public Set<byte[]> queryHashField(byte[] key)
        throws DataCenterException {
        Set<byte[]> set = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            set = jedis.hkeys(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHashField-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return set;
    }

    @Override
    public Long queryHashSize(String key)
        throws DataCenterException {
        Long result = null;
        Jedis jedis = null;
        try {
            jedis = getConnection();
            result = jedis.hlen(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-queryHashSize-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long incr(String key)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.incr(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-incr-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long incr(String key, int value)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.incrBy(key, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-incr-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long decr(String key)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.decr(key);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-decr-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long decr(String key, int value)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.decrBy(key, value);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-decr-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long expire(String key, int seconds)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.expire(key, seconds);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-expire-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Set<String> keys(String pattern)
        throws DataCenterException {
        Jedis jedis = null;
        Set<String> result = null;
        try {
            jedis = getConnection();
            result = jedis.keys(pattern);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + pattern + "]ConfigControl-keys-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long publish(String channel, String message)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.publish(channel, message);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + channel + "]ConfigControl-publish-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public String flushDB()
        throws DataCenterException {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getConnection();
            result = jedis.flushDB();
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[]ConfigControl-flushDB-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }

    @Override
    public Long deleteSortedSetByRank(String key, long start, long end)
        throws DataCenterException {
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = getConnection();
            result = jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            brokenConnection(jedis);
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ConfigControl-deleteSortedSetByScore-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        } finally {
            freeConnection(jedis);
        }
        return result;
    }
}
