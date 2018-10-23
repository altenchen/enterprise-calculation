package storm.dao;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import storm.util.JedisPoolUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Redis 数据访问对象
 * 历史版本中有遗留的Redis存储格式
 *
 * TODO: 通用抽象合并到 JedisPoolUtils
 *
 * @author xzp
 */
public final class DataToRedis implements Serializable {

    private static final long serialVersionUID = -3264877595057681946L;

    private static Logger LOG = LoggerFactory.getLogger(DataToRedis.class);
    private static JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    public DataToRedis() {

    }


    public void saveMap(Map<String, String> map, int db, String table) {
        saveMap(map, db, table, JedisPoolUtils.getInstance().getJedisPool());
    }

    public void saveMap(Map<String, String> map, int db, String table, JedisPool jedisPool) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.hmset(table, map);
            map = null;
        } catch (JedisException e) {
            LOG.error("存储临时统计数据缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("存储临时统计数据缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }
        }

    }

    @Nullable
    public Map<String, String> hashGetAllMapByKeyAndDb(@NotNull final String key, final int db) {

        Map<String, String> map = null;
        try {
            map = JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(db);
                return jedis.hgetAll(key);
            });
        } catch (JedisException e) {
            LOG.error("获取实时数据项值缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("获取实时数据项值缓存异常:" + ex.getMessage(), ex);
        }

        if (MapUtils.isNotEmpty(map)) {
            return map;
        }

        return null;
    }

    public void flushDB(int db) {
        flushDB(db, JedisPoolUtils.getInstance().getJedisPool());
    }

    public void flushDB(int db, JedisPool jedisPool) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.flushDB();
        } catch (JedisException e) {
            LOG.error("存储临时统计数据缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("存储临时统计数据缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
    }

    public Map<String, String> getMap(int db, String name) {
        return getMap(db, name, JedisPoolUtils.getInstance().getJedisPool());
    }

    /**
     * 获取预处理缓存数据map
     *
     * @return
     */
    public Map<String, String> getMap(int db, String name, JedisPool jedisPool) {
        Map<String, String> m = null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            m = jedis.hgetAll(name);
            if (!MapUtils.isEmpty(m)) {
                if (m.size() == 1) {
                    for (Map.Entry<String, String> entry : m.entrySet()) {
                        if (StringUtils.isEmpty(entry.getKey())
                            || StringUtils.isEmpty(entry.getValue())) {
                            m = null;
                        }
                        break;
                    }
                }
            }

        } catch (JedisException e) {
            LOG.error("获取预处理缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("获取预处理缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        if (MapUtils.isEmpty(m)) {
            return null;
        }
        return m;
    }

    public Set<String> getSmembersSet(int db, String name) {

        return getSmembersSet(db, name, JedisPoolUtils.getInstance().getJedisPool());
    }

    public Set<String> getSmembersSet(int db, String name, JedisPool jedisPool) {
        Jedis jedis = null;
        Set<String> smembers = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            smembers = jedis.smembers(name);
        } catch (JedisException e) {
            LOG.error("存储临时统计数据缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("存储临时统计数据缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }

        return smembers;
    }

    public Set<String> getKeysSet(int db, String name) {

        return getKeysSet(db, name, JedisPoolUtils.getInstance().getJedisPool());
    }

    public Set<String> getKeysSet(int db, String name, JedisPool jedisPool) {
        Jedis jedis = null;
        Set<String> keys = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            keys = jedis.keys(name);
        } catch (JedisException e) {
            LOG.error("存储临时统计数据缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("存储临时统计数据缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        return keys;
    }

    public String getString(int db, String name) {

        return getString(db, name, JedisPoolUtils.getInstance().getJedisPool());
    }

    public String getString(int db, String name, JedisPool jedisPool) {
        Jedis jedis = null;
        String string = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            string = jedis.get(name);
        } catch (JedisException e) {
            LOG.error("存储临时统计数据缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("存储临时统计数据缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        return string;
    }

    public void setString(int db, String key, String value) {

        setString(db, key, value, JedisPoolUtils.getInstance().getJedisPool());
    }

    public String setString(int db, String key, String value, JedisPool jedisPool) {
        Jedis jedis = null;
        String string = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            string = jedis.set(key, value);
        } catch (JedisException e) {
            LOG.error("存储临时统计数据缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("存储临时统计数据缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        return string;
    }

    public void hset(int db, String key, String field, String value) {
        hset(db, key, field, value, JedisPoolUtils.getInstance().getJedisPool());
    }

    public void hset(int db, String key, String field, String value, JedisPool jedisPool) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.hset(key, field, value);
        } catch (JedisException e) {
            LOG.error("存储临时统计数据缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("存储临时统计数据缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
    }

    public void hdel(int db, String key, String... field) {
        hdel(db, key, JedisPoolUtils.getInstance().getJedisPool(), field);
    }

    public void hdel(int db, String key, JedisPool jedisPool, String... field) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.hdel(key, field);
        } catch (JedisException e) {
            LOG.error("存储临时统计数据缓存Jedis异常:" + e.getMessage(), e);
        } catch (Exception ex) {
            LOG.error("存储临时统计数据缓存异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
    }


    public void del(int db, String key) {
        del(db, key, JedisPoolUtils.getInstance().getJedisPool());
    }

    public void del(int db, String key, JedisPool jedisPool) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.del(key);
        } catch (Exception ex) {
            LOG.error("redis 删除数据异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
    }

    /**
     * 模糊查询key列表
     * @param db
     * @param keyPrefix
     * @return
     */
    public Set<String> getKeys(int db, String keyPrefix){
        Jedis jedis = null;
        JedisPool jedisPool = JedisPoolUtils.getInstance().getJedisPool();
        try {
            jedis = jedisPool.getResource();
            jedis.select(db);
            Set<String> keys = jedis.keys(keyPrefix + "*");
            return keys;
        } catch (Exception ex) {
            LOG.error("redis 删除数据异常:" + ex.getMessage(), ex);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        return null;
    }

}
