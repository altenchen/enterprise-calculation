package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.dbconn.Conn;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * @author: xzp
 * @date: 2018-06-22
 * @description: 车型缓存
 */
public class VehicleModelCache {

    private static final Logger logger = LoggerFactory.getLogger(VehicleModelCache.class);

    private static final ConfigUtils config = ConfigUtils.getInstance();

    private static final VehicleModelCache INSTANCE = new VehicleModelCache();

    /**
     * 缓存刷新间隔(毫秒), 默认15分钟
     */
    private static long veh_model_cache_refresh_millisecond = 15 * 60 * 1000;

    {
        final Properties sysDefine = config.sysDefine;
        if(sysDefine.containsKey(SysDefine.VEH_MODEL_CACHE_REFRESH_SECOND)) {
            final String property = sysDefine.getProperty(SysDefine.VEH_MODEL_CACHE_REFRESH_SECOND);
            if(StringUtils.isNumeric(property)) {
                try {
                    final long second = Long.parseUnsignedLong(property);
                    veh_model_cache_refresh_millisecond = second * 1000;
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    logger.warn("配置节[{}]不是正整数", SysDefine.VEH_MODEL_CACHE_REFRESH_SECOND);
                }
            } else {
                logger.warn("配置节[{}]不是整数", SysDefine.VEH_MODEL_CACHE_REFRESH_SECOND);
            }
        }
    }

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
            .expireAfterAccess(30, TimeUnit.MINUTES)            .build();
    }

    /**
     * @param vid 车辆Id
     * @return 车辆车型, 空字符串代表没有有效配置
     */
    @NotNull
    public String getVehicleModel(@NotNull String vid) {
        String value = null;
        try {
            value = cache.get(vid, ()-> refreshCache(vid));
        } catch (ExecutionException e) {
            e.printStackTrace();
            logger.warn("更新车辆车型缓存异常");
        }
        return StringUtils.defaultString(value, "");
    }

    private final Object refreshCacheLock = new Object();
    @NotNull
    private String refreshCache(@NotNull String vid) {
        String mid = "";
        final long now = System.currentTimeMillis();
        if(now - lastUpdateTime > veh_model_cache_refresh_millisecond) {
            synchronized (refreshCacheLock) {
                if(now - lastUpdateTime > veh_model_cache_refresh_millisecond) {
                    final Map<String, String> vmd = conn.getVehicleModel();
                    cache.putAll(vmd);
                    mid = vmd.getOrDefault(vid, "");
                    lastUpdateTime = now;
                } else {
                    mid = cache.asMap().getOrDefault(vid, "");
                }
            }
        }
        return mid;
    }
}
