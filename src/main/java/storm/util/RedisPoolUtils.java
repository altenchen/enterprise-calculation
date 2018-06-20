package storm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

/**
 * Redis池工具
 */
public final class RedisPoolUtils {

	private static Logger logger = LoggerFactory.getLogger(RedisPoolUtils.class);
	private static final ConfigUtils CONFIG_UTILS = ConfigUtils.getInstance();

    /** 连接池  */
	private static JedisPool jedisPool = null;

    static {
		try {
			Properties conf = CONFIG_UTILS.sysDefine;
			initRedisConnect(conf);
			logger.info("初始化 redis 线程池");
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

	private static void initRedisConnect(Properties conf) {
		/** 主机地址  */
		String host ;
		/** 端口号	*/
		Integer port=0;
		/** 密码  */
		String pass ;
		/** 超时时间  */
		Integer timeOut = 6000;
		/** 最大连接数  */
		int maxActive = 5000;
		/** 最大空闲的连接数  */
		int maxIdle = 50;
		/** 最大等待时间  */
		int maxWait = 300000;

        host = conf.getProperty("redis.host");
        if (conf.getProperty("redis.port") != null && !isNull(conf.getProperty("redis.port"))) {
            port = Integer.parseInt(conf.getProperty("redis.port"));
        }

        pass = conf.getProperty("redis.pass");
        maxActive = 5000;
        maxIdle = 50;
        if (conf.getProperty("redis.maxWait")!=null && !isNull(conf.getProperty("redis.maxWait"))) {
            maxWait = Integer.parseInt(conf.getProperty("redis.maxWait"));
        }

        if (conf.getProperty("redis.timeOut")!=null && !isNull(conf.getProperty("redis.timeOut"))) {
            timeOut = Integer.parseInt(conf.getProperty("redis.timeOut"));
        }

		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();  
		jedisPoolConfig.setMaxTotal(maxActive); // 控制一个pool最多有多少个状态为idle的jedis实例
        jedisPoolConfig.setMaxIdle(maxIdle);  // 最大能够保持空闲状态的对象数
        jedisPoolConfig.setMaxWaitMillis(maxWait);// 超时时间  单位:毫秒
        jedisPoolConfig.setTestOnBorrow(true); //如果为true，则得到的jedis实例均是可用的
        jedisPoolConfig.setTestOnReturn(true); // 在还会给pool时，是否提前进行validate操作

		if(isNull(pass)){
			jedisPool = new JedisPool(jedisPoolConfig, host, port, timeOut);
		}else {
			jedisPool = new JedisPool(jedisPoolConfig, host, port, timeOut, pass);
		}
		System.out.println("---redis 线程池连接完成");
	}

	private static boolean isNull(String string) {
	        if (null == string 
	        		||"".equals(string.trim())) {
	            return true;
	        }

	        return false;
	}
	
	public static final JedisPool getJedisPool(){
		if(jedisPool == null)
			initRedisConnect(CONFIG_UTILS.sysDefine);
		return jedisPool;
	}

	public static final Jedis getJedis(){
		return jedisPool.getResource();
	}

}
