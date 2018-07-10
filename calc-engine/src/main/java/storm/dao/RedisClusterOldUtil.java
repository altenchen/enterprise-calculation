package storm.dao;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import storm.util.JedisPoolUtils;

public class RedisClusterOldUtil {

    private static Logger logger = LoggerFactory.getLogger(RedisClusterOldUtil.class);

    private static List<RedisClusterOldPool> clusterOldPools;

    static {
        try {
            initCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<RedisClusterOldPool> getPools(){
        return clusterOldPools;
    }

    private static Map<String, String> getRedisClusterInfo(){
        Map<String, String> redisclus = null;
        JedisPool jedisPool = JedisPoolUtils.getInstance().getJedisPool();
        Jedis jedis=null;
        try{
            jedis=jedisPool.getResource();
            jedis.select(0);
            redisclus = jedis.hgetAll("cfg-sys-address");
            System.out.println(redisclus);
        }catch(JedisException e){
            logger.error("存储测试车辆数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储测试车辆数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }
        if (null != redisclus && redisclus.size() >0) {
            return redisclus;
        }
        return null;
    }

    /**
     *
     */
    private static void initCluster(){
        Map<String, String> clusters = getRedisClusterInfo();
        if (null != clusters) {
            clusterOldPools = new LinkedList<RedisClusterOldPool>();
            for (Map.Entry<String, String> entry : clusters.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (null != key && null != value && value.length()>8 && value.indexOf(":")>0) {
                    String [] hostport = value.split(":");
                    if (2 == hostport.length) {
                        Properties clusconf = new Properties();
                        clusconf.setProperty("redis.host", new String(hostport[0]));
                        clusconf.setProperty("redis.port", new String(hostport[1]));

                        RedisClusterOldPool pool = new RedisClusterOldPool(clusconf);
                        clusterOldPools.add(pool);
                    }
                    hostport = null;
                }
            }
        }
    }

}
