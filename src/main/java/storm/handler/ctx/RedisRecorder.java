package storm.handler.ctx;

import java.util.Map;
import java.util.TreeMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import storm.dao.DataToRedis;

public class RedisRecorder implements Recorder {

	DataToRedis redis;
	private void init(DataToRedis redis){
		if (null != redis) {
			this.redis = redis;
		}else{
			
			this.redis = new DataToRedis();
		}
	}
	public RedisRecorder(DataToRedis redis) {
		super();
		init(redis);
	}

	public RedisRecorder() {
		super();
		init(null);
	}

	@Override
	public void save(int db,String type, String id, Map<String, Object> ctx) {
		try {
			String json = JSON.toJSONString(ctx);
			if (null == this.redis) {
				this.redis = new DataToRedis();
			}
			redis.hset(db, type, id, json);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void save(int db,String type, Map<String, Map<String, Object>> ctxs) {
		try {
			if (null == this.redis) {
				this.redis = new DataToRedis();
			}
			for (Map.Entry<String, Map<String, Object>> entry : ctxs.entrySet()) {
				String id = entry.getKey();
				Map<String, Object> ctx = entry.getValue();
				String json = JSON.toJSONString(ctx);
				redis.hset(db, type, id, json);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rebootInit(int db, String type,Map<String, Map<String, Object>> initMap){
		if (null == initMap) {
			throw new RuntimeException("InitMapContainerNullException");
		}
		if (null == this.redis) {
			this.redis = new DataToRedis();
		}
		Map<String, String> redisCache = redis.hgetallMapByKeyAndDb(type, db);
		if (null != redisCache && redisCache.size() > 0) {
			
			for (Map.Entry<String, String> entry : redisCache.entrySet()) {
				String vid = entry.getKey();
				String json = entry.getValue();
				if (null != vid && !"".equals(vid)
						&& null != json && !"".equals(json)) {
					
					Map<String, Object> map  = (Map<String, Object>)JSONObject.parseObject(json, TreeMap.class);
					initMap.put(vid, map);
				}
			}
			
		}
	}

	@Override
	public void del(int db, String type, String ... ids) {
		redis.hdel(db, type, ids);
	}
	
}
