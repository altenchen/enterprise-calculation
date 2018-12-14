package storm.handler.ctx;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Redis记录器
 */
public final class RedisRecorder implements Serializable {

    private static final long serialVersionUID = -2486640129712946392L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisRecorder.class);

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private final DataToRedis redis = new DataToRedis();

    /**
     * 更新哈希表
     * @param dbIndex 数据库索引
     * @param type 表名
     * @param id 项ID
     * @param ctx 上下文
     */
    public void save(int dbIndex, String type, String id, Map<String, Object> ctx) {
        try {
            String json = JSON_UTILS.toJson(ctx);
            redis.hset(dbIndex, type, id, json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新哈希表
     * @param dbIndex 数据库索引
     * @param type 表名
     * @param ctxs field-value pairs
     */
    public void save(int dbIndex, String type, Map<String, Map<String, Object>> ctxs) {
        try {
            for (Map.Entry<String, Map<String, Object>> entry : ctxs.entrySet()) {
                String id = entry.getKey();
                Map<String, Object> ctx = entry.getValue();
                String json = JSON_UTILS.toJson(ctx);
                redis.hset(dbIndex, type, id, json);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新哈希表
     * @param map
     * @param db
     * @param table
     */
    public void saveMap(Map<String, String> map, int db, String table) {
        this.redis.saveMap(map, db, table, JedisPoolUtils.getInstance().getJedisPool());
    }

    /**
     * 从Redis拉取数据初始化initMap
     * @param dbIndex 数据库索引
     * @param type key
     * @param initMap
     */
    public void rebootInit(int dbIndex, String type, Map<String, Map<String, Object>> initMap) {
        if (null == initMap) {
            throw new RuntimeException("InitMapContainerNullException");
        }
        Map<String, String> redisCache = redis.hashGetAllMapByKeyAndDb(type, dbIndex);
        if (MapUtils.isNotEmpty(redisCache)) {

            for (Map.Entry<String, String> entry : redisCache.entrySet()) {
                String vid = entry.getKey();
                String json = entry.getValue();
                if (null != vid && !"".equals(vid)
                    && null != json && !"".equals(json)) {

                    TreeMap<String, Object> map = JSON_UTILS.fromJson(
                        json,
                        new TypeToken<TreeMap<String, Object>>() {
                        }.getType());


                    for (String key : map.keySet()) {
                        final Object value = map.get(key);
                        if(Double.class.equals(value.getClass())) {
                            LOG.debug("[{}]{}: {}-> \"{}\"", dbIndex, type, vid, json);
                        }
                    }


                    initMap.put(vid, map);
                }
            }

        }
    }

    /**
     * 删除哈希项
     * @param dbIndex 数据库索引
     * @param type key
     * @param ids fields
     */
    public void del(int dbIndex, String type, String... ids) {
        redis.hdel(dbIndex, type, ids);
    }

}
