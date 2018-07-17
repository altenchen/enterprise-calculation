package com.ctfo.datacenter.cache.conn;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionPoolFactorySingleton {
    private static Map<Map<String, String>, ShardedJedisPool> shardMap = new ConcurrentHashMap();
    private static Map<String, JedisPool> configMap = new ConcurrentHashMap();

    public static synchronized ShardedJedisPool getShardedJedisPool(Map<String, String> map, ConnConfig connConfig) {
        if (shardMap.get(map) == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(connConfig.getSHARDPOOL_MAXACTIVE());
            config.setMaxIdle(connConfig.getSHARDPOOL_MAXIDLE());
            config.setMaxWaitMillis(connConfig.getSHARDPOOL_MAXWAIT());
            config.setMinIdle(connConfig.getSHARDPOOL_MINIDLE());
            config.setTestOnBorrow(connConfig.isSHARDPOOL_TESTONBORROW());
            List<JedisShardInfo> jedisShardInfoList = new ArrayList();
            for (String name : map.keySet()) {
                final String[] ip_port = ((String) map.get(name)).split(":");
                final int shardPoolTimeout = connConfig.getSHARDPOOL_TIMEOUT();
                JedisShardInfo jedisShardInfo = new JedisShardInfo(
                    ip_port[0],
                    Integer.parseInt(ip_port[1]),
                    shardPoolTimeout,
                    name);
                jedisShardInfoList.add(jedisShardInfo);
            }
            ShardedJedisPool shardedJedisPool = new ShardedJedisPool(config, jedisShardInfoList, Hashing.MD5, Sharded.DEFAULT_KEY_TAG_PATTERN);
            shardMap.put(map, shardedJedisPool);
        }
        return (ShardedJedisPool) shardMap.get(map);
    }

    public static synchronized JedisPool getJedisPool(String address) {
        if (configMap.get(address) == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(1000);
            config.setMaxIdle(100);
            config.setMaxWaitMillis(10000L);
            config.setTestOnBorrow(false);
            config.setMinIdle(0);
            String[] ip_port = address.split(":");
            JedisPool jedisPool = new JedisPool(config, ip_port[0], Integer.parseInt(ip_port[1]), 10000);
            configMap.put(address, jedisPool);
        }
        return (JedisPool) configMap.get(address);
    }
}
