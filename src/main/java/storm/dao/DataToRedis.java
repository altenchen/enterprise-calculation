package storm.dao;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import storm.system.DataKey;
import storm.util.JedisPoolUtils;

/**
 * Redis 数据访问对象
 * @author xzp
 */
public final class DataToRedis {

	private static Logger logger = LoggerFactory.getLogger(DataToRedis.class);

	public void saveVehData(String VID, Object obj){
		saveVehData(JedisPoolUtils.getJedisPool(),VID,obj);
	}

	public void saveVehData(JedisPool jedisPool,String VID, Object obj) {
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
			jedis.select(6);
			jedis.set("DATA_"+VID, JSON.toJSONString(obj));
		}catch(JedisException e){
			logger.error("存储测试车辆数据缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("存储测试车辆数据缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null){
				jedisPool.returnResourceObject(jedis);
			}
		}
	}

	public Object getVehData(String VID){
		return getVehData(JedisPoolUtils.getJedisPool(), VID);
	}
	public Object getVehData(JedisPool jedisPool,String VID) {
        Object obj = null;
        Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
            jedis.select(6);
            String jsonString = jedis.get("DATA_"+VID);
            obj = JSON.parseObject(jsonString, Object.class);
        }catch(JedisException e){
            logger.error("获取测试车辆数据缓存Jedis异常:"+ e.getMessage(), e);
        }catch(Exception ex){
            logger.error("获取测试车辆数据缓存异常:"+ex.getMessage(), ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }

        return obj;
    }
    public void saveVehCmd(String VID, Map<String, String> obj){
    	saveVehCmd(JedisPoolUtils.getJedisPool(),VID, obj);
    }
    public void saveVehCmd(JedisPool jedisPool,String VID, Map<String, String> obj) {
    	Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
            jedis.select(6);
            jedis.set("CMD_"+VID, JSON.toJSONString(obj));
        }catch(JedisException e){
            logger.error("存储测试车辆命令缓存Jedis异常:"+ e.getMessage() ,e);
        }catch(Exception ex){
            logger.error("存储测试车辆命令缓存异常:"+ex.getMessage() ,ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }
    }
    public Map<String, Map<String, String>> getAllVehCMD(){
    	return getAllVehCMD(JedisPoolUtils.getJedisPool());
    }
    public Map<String, Map<String, String>> getAllVehCMD(JedisPool jedisPool) {
        Map<String, Map<String, String>> appCmd = new HashMap<String, Map<String, String>>();
        Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
            jedis.select(6);
            Set<String> keys  = jedis.keys("CMD_*");
            if (keys != null && keys.size() > 0) {
                for(String key : keys){
                    String jsonString = jedis.get(key);
                    Map<String, String> obj = JSON.parseObject(jsonString, Map.class);
                    appCmd.put(new String(key.split("_")[1]), obj);
                    obj=null;
                    key=null;
                }
            }
        }catch(JedisException e){
            logger.error("获取所有测试车辆命令缓存Jedis异常:"+ e.getMessage(), e);
        }catch(Exception ex){
            logger.error("获取所有测试车辆命令缓存异常:"+ex.getMessage(), ex);
        }finally{
            if(jedis != null){
                jedisPool.returnResourceObject(jedis);
            }
        }

        return appCmd;
    }

    public Map<String, Object> getAllVehData(){
    	return getAllVehData(JedisPoolUtils.getJedisPool());
    }
    public Map<String, Object> getAllVehData(JedisPool jedisPool) {
        Map<String, Object> ObjectMap = new HashMap<String, Object>();

        Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
            jedis.select(6);
            Set<String> keys  = jedis.keys("DATA_*");
            if (keys != null && keys.size() > 0) {
                for(String key : keys){
                    String jsonString = jedis.get(key);
                    Object obj = JSON.parseObject(jsonString, Object.class);
                    ObjectMap.put(new String(key.split("_")[1]), obj);
                    obj=null;
                    key=null;
                }
            }
        }catch(JedisException e){
            logger.error("获取所有测试车辆数据缓存Jedis异常:"+ e.getMessage(), e);
        }catch(Exception ex){
            logger.error("获取所有测试车辆数据缓存异常:"+ex.getMessage(), ex);
        }finally{
            if(jedis != null){
            	jedisPool.returnResourceObject(jedis);
            }
        }

        return ObjectMap;
    }
    public void delTestVehInfo(String VID) {
    	delTestVehInfo(JedisPoolUtils.getJedisPool(),VID);
    }
    public void delTestVehInfo(JedisPool jedisPool,String VID) {
    	try {
			Jedis jedis=jedisPool.getResource();
			jedis.select(6);
			jedis.del("DATA_" + VID);
			jedis.del("CMD_" + VID);
			jedisPool.returnResourceObject(jedis);
		} catch (Exception e) {
			e.printStackTrace();
		}

//        jedisPool.destroy();
    }

    public void delAllTestVeh(){
    	delAllTestVeh(JedisPoolUtils.getJedisPool());
    }
    public void delAllTestVeh(JedisPool jedisPool) {
    	try {
			Jedis jedis=jedisPool.getResource();
			jedis.select(6);
			Set<String> keys  = jedis.keys("*");
			if (keys != null && keys.size() > 0) {
			    for(String key : keys){
			        jedis.del(key);
			    }
			}
			jedisPool.returnResourceObject(jedis);
		} catch (Exception e) {
			e.printStackTrace();
		}
//        jedisPool.destroy();
    }
    
    public void saveRealtimeMessage(Map<String,String> map){
    	saveRealtimeMessage(map, JedisPoolUtils.getJedisPool());
    }
    /**
	 * 存储实时数据缓存
	 * @param map
	 * @param jedisPool
	 */
	public void saveRealtimeMessage(Map<String,String> map,JedisPool jedisPool) {
		String vid = map.get(DataKey.VEHICLE_ID);
		Jedis jedis=null;
		try{
			jedis=jedisPool.getResource();
			if(!"".equals(vid) && vid != null){
				jedis.select(1);
				jedis.hmset(vid, map);
			}
		}catch(JedisException e){
			logger.error("存储实时数据缓存Jedis异常:"+map.get(DataKey.VEHICLE_ID)+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("存储实时数据缓存异常:"+map.get(DataKey.VEHICLE_ID)+ex.getMessage() ,ex);
		}
		finally{
			if(jedis != null ){ 
				jedisPool.returnResourceObject(jedis);
			}
		}
	}
	
	public void saveChargeMessage(Map<String,String> map){
		saveChargeMessage(map, JedisPoolUtils.getJedisPool());
	}
	/**
	 * 存储充电缓存
	 * @param map
	 * @param jedisPool
	 */
	public void saveChargeMessage(Map<String,String> map,JedisPool jedisPool) {
		
		Jedis jedis=null;
		try{
			if(map.get(DataKey.VEHICLE_ID) != null){
				jedis = jedisPool.getResource();
				jedis.select(3);
				jedis.hmset(map.get(DataKey.VEHICLE_ID), map);
			}
		}catch(JedisException e){
			logger.error("存储充电数据缓存Jedis异常:"+map.get(DataKey.VEHICLE_ID)+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("存储充电数据缓存异常:"+map.get(DataKey.VEHICLE_ID)+ex.getMessage() ,ex);
		}finally{
			if(jedis != null ){ 
				jedisPool.returnResourceObject(jedis);
			}
		}
	}
	
	public void saveRentcarMessage(Map<String,String> map){
		saveRentcarMessage(map,JedisPoolUtils.getJedisPool());
	}
	
	/**
	 * 存储实时租赁缓存
	 * @param map
	 * @param jedisPool
	 */
	public void saveRentcarMessage(Map<String,String> map,JedisPool jedisPool) {
		
		Jedis jedis=null;
		try{
			if(map.get(DataKey.VEHICLE_ID) != null){
				jedis = jedisPool.getResource();
				jedis.select(2);
				jedis.hmset(map.get(DataKey.VEHICLE_ID), map);
			}
		}catch(JedisException e){
			logger.error("存储租赁数据缓存Jedis异常:"+map.get(DataKey.VEHICLE_ID)+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("存储租赁数据缓存异常:"+map.get(DataKey.VEHICLE_ID)+ex.getMessage() ,ex);
		}finally{
			if(jedis != null ){ 
				jedisPool.returnResourceObject(jedis);
			}
		}
	}
	
	public Map<String,String> getFilterMap(){
		return getFilterMap(JedisPoolUtils.getJedisPool());
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
			if (!MapUtils.isEmpty(m))
				if(m.size()==1)
					for (Map.Entry<String,String> entry : m.entrySet()) {
						if(StringUtils.isEmpty(entry.getKey())
								|| StringUtils.isEmpty(entry.getValue()))
							m=null;
						break;
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
		if (MapUtils.isEmpty(m))
			return null;
		return m;
	}
	
	public Map<String,Set<String>> getAlarmMap(){
		return getAlarmMap(JedisPoolUtils.getJedisPool());
	}
	/**
	 * 
	 *获取预警缓存数据map
	 * @return
	 */
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
							if(!CollectionUtils.isEmpty(defaultAlarm))
								value.addAll(defaultAlarm);
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
		if (MapUtils.isEmpty(s))
			return null;
		return s;
	}
	
	public Set<String> smembers(int db,String name){

		return smembers(db,name,JedisPoolUtils.getJedisPool());
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
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}

		return smembers;
	}
	
	public void delKeys(Set<String> keys,int db){
		if(null != keys && keys.size()>0)
			delKeys(keys,db,JedisPoolUtils.getJedisPool());
	}
	public void delKeys(Set<String> keys,int db,JedisPool jedisPool) {
		if(null == keys || keys.size()<1)
			return;
		Jedis jedis = null ;
		try{
			jedis = jedisPool.getResource();
			jedis.select(db);
			for (String key : keys) {
				if(null !=key && !"".equals(key.trim()))
					jedis.del(key);
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
	public void delKey(String key,int db){
		if(null != key )
			delKey(key,db,JedisPoolUtils.getJedisPool());
	}
	public void delKey(String key,int db,JedisPool jedisPool) {
		if(null == key)
			return;
		Jedis jedis = null ;
		try{
			jedis = jedisPool.getResource();
			jedis.select(db);
			jedis.del(key);
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
		return getValueByDataId(vid,dataId,JedisPoolUtils.getJedisPool());
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
		saveStatisticsMessage(map,JedisPoolUtils.getJedisPool());
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

	
	public List<String> getVehicleStatusList(String dataId,String utcId){
		return getVehicleStatusList(dataId,utcId,JedisPoolUtils.getJedisPool());
	}
	/**
	 * 定时任务获取车辆在线/故障状态
	 * @return
	 */
	public List<String> getVehicleStatusList(String dataId,String utcId,JedisPool jedisPool) {		
		List<String> list = new LinkedList<String>();
		Set<String> keys = null;
		Jedis jedis = null;
		try{			
			jedis = jedisPool.getResource();
			jedis.select(1);
			keys = jedis.keys("*");
			for(String vid : keys){
				if("1".equals(jedis.hget(vid, dataId))){
					String utc = jedis.hget(vid, utcId);
					utc= StringUtils.isEmpty(utc) ?"0":utc;
					list.add(vid+":"+utc);
				}
			}
		}catch(JedisException e){
			logger.error("定时任务获取车辆"+dataId+"状态缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("定时任务获取车辆"+dataId+"状态缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null){
				jedisPool.returnResourceObject(jedis);
			}
		}
		
		return list;
	}
	
	public void updateStatusByDataId(List<String> vidList,String dataId){
		updateStatusByDataId(vidList,dataId,JedisPoolUtils.getJedisPool());
	}
	/**
	 * 定时任务更新车辆[在线]/[故障]状态
	 * @param vidList
	 * @param dataId
	 * @param jedisPool
	 */
	public void updateStatusByDataId(List<String> vidList,String dataId,JedisPool jedisPool){
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			jedis.select(1);
			for(String vid : vidList){
				jedis.hset(vid, dataId, "0"); 
			}	
		}catch(JedisException e){
			logger.error("定时任务更新车辆"+dataId+"状态缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("定时任务更新车辆"+dataId+"状态缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null){ 
				jedisPool.returnResourceObject(jedis);
			}
		}
	}
	public void saveTotalMessage(Map<String,String> map){
		saveTotalMessage(map,JedisPoolUtils.getJedisPool());
	}
	public void saveTotalMessage(Map<String,String> map,JedisPool jedisPool) {
		Jedis jedis = null ;
		try{
			jedis = jedisPool.getResource();
			jedis.select(7);
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
		saveMap(map,db,table,JedisPoolUtils.getJedisPool());
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
		return hgetBykeyAndFiled(key,filed,db,JedisPoolUtils.getJedisPool());
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
		return hgetallMapByKeyAndDb(key, db,JedisPoolUtils.getJedisPool());
	}
	public Map<String,String> hgetallMapByKeyAndDb(String key,int db,JedisPool jedisPool){
		Map<String,String> map  =null;
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			jedis.select(db);
			map=jedis.hgetAll(key);
		}catch(JedisException e){
			logger.error("获取实时数据项值缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("获取实时数据项值缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null){
				jedisPool.returnResourceObject(jedis);
			}
		}

		if (null != map && map.size() >0) {
			return map;
		}
		return null;
	}
	
	public void flushDB(int db){
		flushDB(db,JedisPoolUtils.getJedisPool());
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
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}
	}

	public Map<String,String> getMap(int db,String name){
		return getMap(db,name,JedisPoolUtils.getJedisPool());
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
			if (!MapUtils.isEmpty(m))
				if(m.size()==1)
					for (Map.Entry<String,String> entry : m.entrySet()) {
						if(StringUtils.isEmpty(entry.getKey())
								|| StringUtils.isEmpty(entry.getValue()))
							m=null;
						break;
					}
			
		}catch(JedisException e){
			logger.error("获取预处理缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("获取预处理缓存异常:"+ex.getMessage() ,ex);
		} finally{
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}
		if (MapUtils.isEmpty(m))
			return null;
		return m;
	}

	public Set<String> getSmembersSet(int db,String name){

		return getSmembersSet(db,name,JedisPoolUtils.getJedisPool());
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
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}

		return smembers;
	}
	public Set<String> getKeysSet(int db,String name){
		
		return getKeysSet(db,name,JedisPoolUtils.getJedisPool());
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
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}
		return keys;
	}
	
public String getString(int db,String name){
		
		return getString(db,name,JedisPoolUtils.getJedisPool());
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
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}
		return string;
	}
	
	public void setString(int db,String key,String value){
		
		setString(db,key,value,JedisPoolUtils.getJedisPool());
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
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}
		return string;
	}
	
	public Set<String> subhkeys(int db,String key){
		return subhkeys(db, key, JedisPoolUtils.getJedisPool());
	}
	
	public Set<String> subhkeys(int db,String key,JedisPool jedisPool){
		Jedis jedis = null ;
		Set<String> keys=null;
		try{
			jedis = jedisPool.getResource();
			jedis.select(db);
			keys =jedis.hkeys(key);
		}catch(JedisException e){
			logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}
		return keys;
	}
	
	public void incrKey(int db,String key){
		incrKey(db,key,JedisPoolUtils.getJedisPool());
	}
	public void incrKey(int db,String key,JedisPool jedisPool) {
		Jedis jedis = null ;
		try{
			jedis = jedisPool.getResource();
			jedis.select(db);
			jedis.incr(key);
		}catch(JedisException e){
			logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);

		}
	}
	
	public void incrByKey(int db,String key,long value){
		incrByKey(db,key,value,JedisPoolUtils.getJedisPool());
	}
	public void incrByKey(int db,String key,long value,JedisPool jedisPool) {
		Jedis jedis = null ;
		try{
			jedis = jedisPool.getResource();
			jedis.select(db);
			jedis.incrBy(key,value);
		}catch(JedisException e){
			logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);
			
		}
	}
	
	public void hset(int db,String key,String field,String value){
		hset(db, key, field, value,JedisPoolUtils.getJedisPool());
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
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);
			
		}
	}
	public void hset(int db,String key,List<String[]>fieldValues){
		hset(db, key, fieldValues,JedisPoolUtils.getJedisPool());
	}
	public void hset(int db,String key,List<String[]>fieldValues,JedisPool jedisPool) {
		Jedis jedis = null ;
		try{
			jedis = jedisPool.getResource();
			jedis.select(db);
			for (String[] fieldValue : fieldValues) {
				if (null != fieldValue && fieldValue.length >= 2) {
					if (null !=fieldValue[0] && null !=fieldValue[1]) {
						
						jedis.hset(key, fieldValue[0], fieldValue[1]);
					}
				}
			}
		}catch(JedisException e){
			logger.error("存储临时统计数据缓存Jedis异常:"+ e.getMessage() ,e);
		}catch(Exception ex){
			logger.error("存储临时统计数据缓存异常:"+ex.getMessage() ,ex);
		}finally{
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);
			
		}
	}
	
	public void hdel(int db,String key,String ... field){
		hdel(db, key, JedisPoolUtils.getJedisPool(), field);
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
			if(jedis != null)
				jedisPool.returnResourceObject(jedis);
			
		}
	}
}
