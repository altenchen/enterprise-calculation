package storm.dao;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
import storm.util.ConfigUtils;

public class RedisClusterOldPool {
	private static Logger logg = LoggerFactory.getLogger(RedisClusterOldPool.class);
	private static final ConfigUtils configUtils = ConfigUtils.getInstance();
    /** 连接池  */
	private JedisPool jedisPool = null;
	private Properties conf;
//	private static RedisClusterOldPool instance ;
//	public synchronized static RedisClusterOldPool getInstance(Properties clusconf){
//		if (null == instance) {
//			synchronized (RedisClusterOldPool.class) {
//				if (null == instance)
//					instance = new RedisClusterOldPool(clusconf);
//			}
//		}
//		return instance;
//	}
    RedisClusterOldPool(Properties clusconf) {
		super();
		init(clusconf);
	}
	private void init(Properties clusconf){
		try {
			Properties sysconf = configUtils.sysDefine;
			conf = new Properties();
			conf.setProperty("redis.host", clusconf.getProperty("redis.host"));
			conf.setProperty("redis.port", clusconf.getProperty("redis.port"));
			conf.setProperty("redis.pass", sysconf.getProperty("redis.pass"));
			conf.setProperty("redis.maxWait", sysconf.getProperty("redis.maxWait"));
			conf.setProperty("redis.timeOut", sysconf.getProperty("redis.timeOut"));
			initRedisConn(conf);
			logg.info("初始化 redis 线程池");
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

	private void initRedisConn(Properties conf) {
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
	private boolean isNull(String string) {
	        if (null == string 
	        		||"".equals(string.trim())) {
	            return true;
	        }

	        return false;
	}
	
	public final JedisPool getJedisPool(){
		if(jedisPool == null) {
			initRedisConn(configUtils.sysDefine);
		}
		return jedisPool;
	}
	public final Jedis getJedis(){
		return jedisPool.getResource();
	}
	
	public final Set<String> keys(int db,String key){
		JedisPool jedisPool = getJedisPool();
		if (null == jedisPool) {
			return null;
		}
		return keys(jedisPool, db, key);
	}
	
	private Set<String> keys(JedisPool jedisPool,int db,String key){
		Set<String> keysets = null;
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
			jedis.select(0);
			keysets = jedis.keys(key);
			
		}catch(JedisException e){
			logg.error("存储测试车辆数据缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logg.error("存储测试车辆数据缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null){ 
				jedisPool.returnResourceObject(jedis);
			}
		}
		if (null != keysets && keysets.size() >0) {
			return keysets;
		}
		return null;
	}
	
	public final Map<String, String> hgetall(int db,String key){
		JedisPool jedisPool = getJedisPool();
		if (null == jedisPool) {
			return null;
		}
		return hgetall(jedisPool, db, key);
	}
	
	private Map<String, String> hgetall(JedisPool jedisPool,int db,String key){
		Map<String, String> dat = null;
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
			jedis.select(db);
			dat = jedis.hgetAll(key);
			
		}catch(JedisException e){
			logg.error("存储测试车辆数据缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logg.error("存储测试车辆数据缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null){ 
				jedisPool.returnResourceObject(jedis);
			}
		}
		if (null != dat && dat.size() >0) {
			return dat;
		}
		return null;
	}
	
	public final String hget(int db,String key,String field){
		JedisPool jedisPool = getJedisPool();
		if (null == jedisPool) {
			return null;
		}
		return hget(jedisPool, db, key, field);
	}
	
	private String hget(JedisPool jedisPool,int db,String key,String field){
		String dat = null;
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
			jedis.select(db);
			dat = jedis.hget(key, field);
			
		}catch(JedisException e){
			logg.error("存储测试车辆数据缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logg.error("存储测试车辆数据缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null){ 
				jedisPool.returnResourceObject(jedis);
			}
		}
		if (null != dat && !"".equals(dat)) {
			return dat;
		}
		return null;
	}
	
}
