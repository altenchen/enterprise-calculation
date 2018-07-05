package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.JedisPoolUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 车辆数据断面, 缓存了车辆的部分数据的最后有效值.
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
public final class VehDataSurface {

    private static final Logger logger = LoggerFactory.getLogger(VehDataSurface.class);

    private static final VehDataSurface INSTANCE = new VehDataSurface();

    @Contract(pure = true)
    public static VehDataSurface getInstance() {
        return INSTANCE;
    }

    private static final int REDIS_DB_INDEX = 6;

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    @NotNull
    private static final String buildTable(@NotNull String vid) {
        return "veh.data.surface." + vid;
    }

    /**
     * <vid, <key, value>>
     */
    private static final Cache<String, Map<String, String>> cacheTable;

    static {

        cacheTable = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Map<String, String>>() {
                @Override
                public Map<String, String> load(String s) {
                    final Map<String, String> cache = new HashMap<>();
                    JEDIS_POOL_UTILS.useResource(jedis -> {
                        final String select = jedis.select(REDIS_DB_INDEX);
                    });
                    return cache;
                }
            });
    }
}
