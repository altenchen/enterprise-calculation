package storm.handler.ctx;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.util.JsonUtils;

import java.util.Map;
import java.util.TreeMap;

/**
 * Redis记录器实现
 */
public final class RedisRecorder implements Recorder {

    private static final Logger logger = LoggerFactory.getLogger(RedisRecorder.class);

    private static final JsonUtils gson = JsonUtils.getInstance();

    private DataToRedis redis;

    private void init(DataToRedis redis) {
        if (null != redis) {
            this.redis = redis;
        } else {

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
    public void save(int dbIndex, String type, String id, Map<String, Object> ctx) {
        try {
            String json = gson.toJson(ctx);
            if (null == this.redis) {
                this.redis = new DataToRedis();
            }
            redis.hset(dbIndex, type, id, json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void save(int dbIndex, String type, Map<String, Map<String, Object>> ctxs) {
        try {
            if (null == this.redis) {
                this.redis = new DataToRedis();
            }
            for (Map.Entry<String, Map<String, Object>> entry : ctxs.entrySet()) {
                String id = entry.getKey();
                Map<String, Object> ctx = entry.getValue();
                String json = gson.toJson(ctx);
                redis.hset(dbIndex, type, id, json);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void rebootInit(int dbIndex, String type, Map<String, Map<String, Object>> initMap) {
        if (null == initMap) {
            throw new RuntimeException("InitMapContainerNullException");
        }
        if (null == this.redis) {
            this.redis = new DataToRedis();
        }
        Map<String, String> redisCache = redis.hashGetAllMapByKeyAndDb(type, dbIndex);
        if (MapUtils.isNotEmpty(redisCache)) {

            for (Map.Entry<String, String> entry : redisCache.entrySet()) {
                String vid = entry.getKey();
                String json = entry.getValue();
                if (null != vid && !"".equals(vid)
                    && null != json && !"".equals(json)) {

                    TreeMap<String, Object> map = gson.fromJson(
                        json,
                        new TypeToken<TreeMap<String, Object>>() {
                        }.getType());


                    for (String key : map.keySet()) {
                        final Object value = map.get(key);
                        if(Double.class.equals(value.getClass())) {
                            logger.debug("[{}]{}: {}-> \"{}\"", dbIndex, type, vid, json);
                        }
                    }


                    initMap.put(vid, map);
                }
            }

        }
    }

    @Override
    public void del(int dbIndex, String type, String... ids) {
        redis.hdel(dbIndex, type, ids);
    }

}
