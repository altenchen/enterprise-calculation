package com.ctfo.datacenter.cache.conn.impl;

import com.ctfo.datacenter.cache.conn.ShardControl;
import com.ctfo.datacenter.cache.conn.ShardPipelineControl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;

import java.util.List;
import java.util.Map;

public class ShardPipelineControlImpl
    implements ShardPipelineControl {
    private ShardedJedisPool shardedJedisPool = null;
    private ShardedJedis shardedJedis = null;
    private ShardControl shardControl = null;
    private ShardedJedisPipeline shardedJedisPipeline = null;

    public ShardPipelineControlImpl(ShardedJedisPool shardedJedisPool) {
        this.shardedJedis = ((ShardedJedis) shardedJedisPool.getResource());
        this.shardedJedisPipeline = this.shardedJedis.pipelined();
    }

    public ShardPipelineControlImpl(ShardControl shardControl) {
        this.shardControl = shardControl;
        this.shardedJedis = shardControl.getConnection();
        this.shardedJedisPipeline = this.shardedJedis.pipelined();
    }

    @Override
    public void reconnectPipeline()
        throws DataCenterException {
        try {
            this.shardedJedisPipeline = this.shardedJedis.pipelined();
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[]ShardPipelineControlImpl-reconnectPipeline-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void disconnect()
        throws DataCenterException {
        try {
            if (this.shardedJedis != null) {
                if (this.shardControl != null) {
                    this.shardControl.freeConnection(this.shardedJedis);
                } else {
                    this.shardedJedisPool.returnResource(this.shardedJedis);
                }
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[]ShardPipelineControlImpl-disconnect-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void delete(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.del(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[]ShardPipelineControlImpl-delete-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void close()
        throws DataCenterException {
        try {
            if (this.shardedJedis != null) {
                if (this.shardControl != null) {
                    this.shardControl.freeConnection(this.shardedJedis);
                } else {
                    this.shardedJedisPool.returnResource(this.shardedJedis);
                }
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[]ShardPipelineControlImpl-close-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void sync()
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.sync();
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[]ShardPipelineControlImpl-sync-error:" + e.getMessage());
            dataCenterException.setExceptionCode("001");
            throw dataCenterException;
        }
    }

    @Override
    public List<Object> syncAndReturnAll()
        throws DataCenterException {
        try {
            return this.shardedJedisPipeline.syncAndReturnAll();
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[]ShardPipelineControlImpl-syncAndReturnAll-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void add(String key, int seconds, String value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.set(key, value);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-add-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void add(byte[] key, int seconds, byte[] value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.set(key, value);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-add-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addRList(String key, int seconds, String... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.rpush(key, values);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addRList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addRList(byte[] key, int seconds, byte[]... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.rpush(key, values);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addRList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addLList(String key, int seconds, String... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.lpush(key, values);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addLList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addLList(byte[] key, int seconds, byte[]... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.lpush(key, values);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addLList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addSet(String key, int seconds, String... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.sadd(key, values);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addSet-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addSet(byte[] key, int seconds, byte[]... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.sadd(key, values);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addset-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addSortedSet(String key, int seconds, double score, String value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zadd(key, score, value);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addSortedSet-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addSortedSet(byte[] key, int seconds, double score, byte[] value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zadd(key, score, value);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addSortedSet-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addHash(String key, int seconds, String field, String value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hset(key, field, value);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addHash(byte[] key, int seconds, byte[] field, byte[] value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hset(key, field, value);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addHash(String key, int seconds, Map<String, String> values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hmset(key, values);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]hardPipelineControlImpl-addHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void addHash(byte[] key, int seconds, Map<byte[], byte[]> values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hmset(key, values);
            if (seconds > 0) {
                this.shardedJedisPipeline.expire(key, seconds);
            }
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-addHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void expire(String key, int seconds)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.expire(key, seconds);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-expire-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryObjectSortedSet(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zrevrangeWithScores(key, 0L, -1L);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryObjectSortedSet-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryType(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.type(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryType-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryTTL(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.ttl(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryTTL-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void query(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.get(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-query-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void query(byte[] key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.get(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-query-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryList(String key, Long start, Long end)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.lrange(key, start.longValue(), end.longValue());
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryList(byte[] key, int start, int end)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.lrange(key, start, end);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void popRList(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.rpop(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-popRList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void popRList(byte[] key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.rpop(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-popRList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void popLList(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.lpop(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-popRList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void popLList(byte[] key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.lpop(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-popLList-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryListSize(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.llen(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryListSize-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void querySet(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.smembers(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-querySet-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void querySet(byte[] key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.smembers(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-querySet-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void querySetSize(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.scard(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-querySetSize-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void querySortedSet(String key, long start, long end)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zrange(key, start, end);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-querySortedSet-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void querySortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zrangeByScore(key, start, end);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-querySortedSetByScore-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void querySortedSet(byte[] key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zrange(key, 0L, -1L);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-querySortedSet-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void querySortedSetSize(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zcard(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-querySortedSetSize-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHash(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hgetAll(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHash(byte[] key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hgetAll(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHashField(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hkeys(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHashField-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHashField(byte[] key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hkeys(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHashField-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHash(String key, String field)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hget(key, field);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHash(byte[] key, byte[] field)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hget(key, field);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHash(String key, String... fields)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hmget(key, fields);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHash(byte[] key, byte[]... fields)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hmget(key, fields);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHash-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void queryHashSize(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hlen(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-queryHashSize-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void isSetValueExist(String key, String value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.sismember(key, value);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-isSetValueExist-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void isKeyExist(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.exists(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-isKeyExist-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteHashField(String key, String... fields)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hdel(key, fields);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteHashField-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteHashField(byte[] key, byte[]... fields)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.hdel(key, fields);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteHashField-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteSetValue(String key, String... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.srem(key, values);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteSetValue-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteSetValue(byte[] key, byte[]... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.srem(key, values);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteSetValue-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteSortedSetByRank(String key, long start, long end)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteSortedSetByRank-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteSortedSetByScore(String key, double start, double end)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteSortedSetByScore-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteSortedSetValue(String key, String... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zrem(key, values);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteSortedSetValue-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteSortedSetValue(byte[] key, byte[]... values)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.zrem(key, values);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteSortedSetValue-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteListValue(String key, long count, String value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.lrem(key, count, value);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteListValue-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void deleteListValue(byte[] key, int count, byte[] value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.lrem(key, count, value);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-deleteListValue-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void incr(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.incr(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-incr-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void incr(String key, int value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.incrBy(key, value);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-incr-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void decr(String key)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.decr(key);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-decr-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }

    @Override
    public void decr(String key, int value)
        throws DataCenterException {
        try {
            this.shardedJedisPipeline.decrBy(key, value);
        } catch (Exception e) {
            DataCenterException dataCenterException = new DataCenterException("[" + key + "]ShardPipelineControlImpl-decr-error:" + e.getMessage());

            dataCenterException.setExceptionCode("001");

            throw dataCenterException;
        }
    }
}
