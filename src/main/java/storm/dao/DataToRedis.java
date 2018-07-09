package storm.dao;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import storm.util.JedisPoolUtils;

/**
 * Redis 数据访问对象
 * 历史版本中有遗留的Redis存储格式
 * @author xzp
 */
public final class DataToRedis {

    private static Logger logger = LoggerFactory.getLogger(DataToRedis.class);
    private static JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    public DataToRedis() {

    }

    public Map<String,String> getFilterMap(){
        return getFilterMap(JedisPoolUtils.getInstance().getJedisPool());
    }
    /**
     *
     *获取预处理缓存数据map
     * @return
     */
    public Map<String,String> getFilterMap(JedisPool jedisPool){
        Map<String,String> m  = null;
        Jedis jedis = null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(4);
            m = jedis.hgetAll("XNY.FILTER");
            if (!MapUtils.isEmpty(m)) {
                if(m.size()==1) {
                    for (Map.Entry<String, String> entry : m.entrySet()) {
                        if (StringUtils.isEmpty(entry.getKey())
                            || StringUtils.isEmpty(entry.getValue())) {
                            m = null;
                        }
                        break;
                    }
                }
            }

        }catch(JedisException e){
            logger.error("获取预处理缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("获取预处理缓存异常:"+ex.getMessage() ,ex);
        } finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }
        if (MapUtils.isEmpty(m)) {
            return null;
        }
        return m;
    }

    public Map<String,Set<String>> getAlarmMap(){
        return getAlarmMap(JedisPoolUtils.getInstance().getJedisPool());
    }
    /**
     *
     *获取预警缓存数据map
     * @return
     */
    @Nullable
    public Map<String,Set<String>> getAlarmMap(JedisPool jedisPool){
        Map<String,Set<String>> s  = new HashMap<String, Set<String>>();
        Set<String> keys  = null;
        Jedis jedis  = null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(4);
            Set<String> defaultAlarm = jedis.smembers("XNY.ALARM");

            keys = jedis.keys("XNY.ALARM_*");
            if (!CollectionUtils.isEmpty(keys)) {
                for(String key : keys){
                    if (!StringUtils.isEmpty(key)) {
                        Set<String> value = jedis.smembers(key);

                        if (!CollectionUtils.isEmpty(value)) {
                            if(!CollectionUtils.isEmpty(defaultAlarm)) {
                                value.addAll(defaultAlarm);
                            }
                            s.put(key.split("_")[1],value);
                        }

                    }
                }
            }

        }catch(JedisException e){
            logger.error("获取软报警缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("获取软报警缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }
        if (MapUtils.isEmpty(s)) {
            return null;
        }
        return s;
    }

    public Set<String> smembers(int db,String name){

        return smembers(db,name, JedisPoolUtils.getInstance().getJedisPool());
    }
    public Set<String> smembers(int db,String name,JedisPool jedisPool){
        Jedis jedis = null ;
        Set<String> smembers=null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            smembers = jedis.smembers(name);
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }

        return smembers;
    }

    public void delKeys(Set<String> keys,int db){
        if(null != keys && keys.size()>0) {
            delKeys(keys,db, JedisPoolUtils.getInstance().getJedisPool());
        }
    }
    public void delKeys(Set<String> keys,int db,JedisPool jedisPool) {
        if(null == keys || keys.size()<1) {
            return;
        }
        Jedis jedis = null ;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            for (String key : keys) {
                if(null !=key && !"".equals(key.trim())) {
                    jedis.del(key);
                }
            }
            keys=null;
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }

    }

    public String getValueByDataId(String vid,String dataId){
        return getValueByDataId(vid,dataId, JedisPoolUtils.getInstance().getJedisPool());
    }
    /**
     *
     *获取录入车辆数
     * @return
     */
    public String getValueByDataId(String vid,String dataId,JedisPool jedisPool){
        String s  ="";
        Jedis jedis = null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(5);
            s = jedis.hget(vid,dataId);
        }catch(JedisException e){
            logger.error("获取实时数据项值缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("获取实时数据项值缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }

        return s;
    }


    public void saveStatisticsMessage(Map<String,String> map){
        saveStatisticsMessage(map, JedisPoolUtils.getInstance().getJedisPool());
    }
    /**
     * 存储特定数据项的实时数据
     * @param map
     * @param jedisPool
     */
    public void saveStatisticsMessage(Map<String,String> map,JedisPool jedisPool) {
        Jedis jedis = null ;
        try{
            jedis = jedisPool.getResource();
            jedis.select(5);
            jedis.hmset("TOTAL_DATA",map);
            map=null;
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }

    }


    public void saveMap(Map<String,String> map,int db,String table){
        saveMap(map,db,table, JedisPoolUtils.getInstance().getJedisPool());
    }
    public void saveMap(Map<String,String> map,int db,String table,JedisPool jedisPool) {
        Jedis jedis = null ;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.hmset(table,map);
            map=null;
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }

    }

    public String hgetBykeyAndFiled(String key,String filed,int db){
        return hgetBykeyAndFiled(key,filed,db, JedisPoolUtils.getInstance().getJedisPool());
    }
    /**
     *
     *获取录入车辆数
     * @return
     */
    public String hgetBykeyAndFiled(String key,String filed,int db,JedisPool jedisPool){
        String s  = null;
        Jedis jedis = null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            s = jedis.hget(key,filed);
        }catch(JedisException e){
            logger.error("获取实时数据项值缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("获取实时数据项值缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }
        if (null != s && !"".equals(s)) {
            return s;
        }
        return null;
    }

    public Map<String,String> hgetallMapByKeyAndDb(String key,int db){
        Map<String,String> map  =null;
        try{
            map = JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(db);
                return jedis.hgetAll(key);
            });
        } catch(JedisException e) {
            logger.error("获取实时数据项值缓存Jedis异常:"+ e.getMessage() ,e);
        } catch(Exception ex) {
            logger.error("获取实时数据项值缓存异常:"+ex.getMessage() ,ex);
        }

        if (MapUtils.isNotEmpty(map)) {
            return map;
        }
        return null;
    }

    public void flushDB(int db){
        flushDB(db, JedisPoolUtils.getInstance().getJedisPool());
    }
    public void flushDB(int db,JedisPool jedisPool) {
        Jedis jedis = null ;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.flushDB();
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
    }

    public Map<String,String> getMap(int db,String name){
        return getMap(db,name, JedisPoolUtils.getInstance().getJedisPool());
    }
    /**
     *
     *获取预处理缓存数据map
     * @return
     */
    public Map<String,String> getMap(int db,String name,JedisPool jedisPool){
        Map<String,String> m  = null;
        Jedis jedis = null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            m = jedis.hgetAll(name);
            if (!MapUtils.isEmpty(m)) {
                if(m.size()==1) {
                    for (Map.Entry<String, String> entry : m.entrySet()) {
                        if (StringUtils.isEmpty(entry.getKey())
                            || StringUtils.isEmpty(entry.getValue())) {
                            m = null;
                        }
                        break;
                    }
                }
            }

        }catch(JedisException e){
            logger.error("获取预处理缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("获取预处理缓存异常:"+ex.getMessage() ,ex);
        } finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        if (MapUtils.isEmpty(m)) {
            return null;
        }
        return m;
    }

    public Set<String> getSmembersSet(int db,String name){

        return getSmembersSet(db,name, JedisPoolUtils.getInstance().getJedisPool());
    }
    public Set<String> getSmembersSet(int db,String name,JedisPool jedisPool){
        Jedis jedis = null ;
        Set<String> smembers=null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            smembers = jedis.smembers(name);
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }

        return smembers;
    }
    public Set<String> getKeysSet(int db,String name){

        return getKeysSet(db,name, JedisPoolUtils.getInstance().getJedisPool());
    }

    public Set<String> getKeysSet(int db,String name,JedisPool jedisPool){
        Jedis jedis = null ;
        Set<String> keys=null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            keys = jedis.keys(name);
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        return keys;
    }

public String getString(int db,String name){

    return getString(db,name, JedisPoolUtils.getInstance().getJedisPool());
    }

    public String getString(int db,String name,JedisPool jedisPool){
        Jedis jedis = null ;
        String string=null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            string = jedis.get(name);
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        return string;
    }

    public void setString(int db,String key,String value){

        setString(db,key,value, JedisPoolUtils.getInstance().getJedisPool());
    }

    public String setString(int db,String key,String value,JedisPool jedisPool){
        Jedis jedis = null ;
        String string=null;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            string = jedis.set(key, value);
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
        return string;
    }

    public void hset(int db,String key,String field,String value){
        hset(db, key, field, value, JedisPoolUtils.getInstance().getJedisPool());
    }
    public void hset(int db,String key,String field,String value,JedisPool jedisPool) {
        Jedis jedis = null ;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.hset(key, field, value);
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
    }

    public void hdel(int db,String key,String ... field){
        hdel(db, key, JedisPoolUtils.getInstance().getJedisPool(), field);
    }
    public void hdel(int db,String key,JedisPool jedisPool,String ... field) {
        Jedis jedis = null ;
        try{
            jedis = jedisPool.getResource();
            jedis.select(db);
            jedis.hdel(key, field);
        }catch(JedisException e){
            logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }

        }
    }
}
