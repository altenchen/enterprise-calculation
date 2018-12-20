package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.ConfigUtils;
import storm.util.dbconn.Conn;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;


/**
 * @author: xzp
 * @date: 2018-06-22
 * @description: 车型缓存
 */
public class VehicleModelCache {

    private static final Logger logger = LoggerFactory.getLogger(VehicleModelCache.class);

    private static final VehicleModelCache INSTANCE = new VehicleModelCache();

    @Contract(pure = true)
    public static VehicleModelCache getInstance() {
        return INSTANCE;
    }

    private final Conn conn = new Conn();

    /**
     * 最后更新时间
     */
    private long lastUpdateTime = 0;

    private final Cache<String, String> cache;

    private VehicleModelCache() {
        if (INSTANCE != null) {
            throw new IllegalStateException();
        }

        cache = CacheBuilder.newBuilder()
            .expireAfterWrite(60, TimeUnit.MINUTES)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build();
    }

    /**
     * @param vid 车辆Id
     * @return 车辆车型, 空字符串代表没有有效配置
     */
    @NotNull
    public String getVehicleModel(@NotNull String vid) {
        final ConcurrentMap<String, String> map = cache.asMap();
        if(map.containsKey(vid)) {
            return StringUtils.defaultString(map.get(vid), "");
        } else {
            return refreshCache(vid);
        }
    }

    private final Object refreshCacheLock = new Object();
    @NotNull
    private String refreshCache(@NotNull String vid) {
        String mid = "";
        final long now = System.currentTimeMillis();
        long veh_model_cache_refresh_millisecond = ConfigUtils.getSysDefine().getDbCacheFlushtime() * 1000;
        if(now - lastUpdateTime > veh_model_cache_refresh_millisecond) {
            synchronized (refreshCacheLock) {
                if(now - lastUpdateTime > veh_model_cache_refresh_millisecond) {
                    logger.info("开始更新车辆车型信息");
                    final Map<String, String> vmd = conn.getVehicleModel();
                    vmd.forEach((k,v) -> cache.put(k, v));
                    mid = vmd.getOrDefault(vid, "");
                    lastUpdateTime = now;
                    logger.info("更新车辆车型信息完毕, 共获取到[{}]条数据", vmd.size());
                } else {
                    mid = cache.asMap().getOrDefault(vid, "");
                }
            }
        }
        return mid;
    }
}
