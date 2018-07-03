package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ent.calc.util.JedisPoolUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 车辆数据缓存, 缓存了车辆的部分数据的最后有效值.
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
public final class VehDataStream {

    private static final Logger logger = LoggerFactory.getLogger(VehDataStream.class);

    private static final VehDataStream INSTANCE = new VehDataStream();

    @Contract(pure = true)
    public static VehDataStream getInstance() {
        return INSTANCE;
    }

    private static final int REDIS_DB_INDEX = 6;

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    @NotNull
    private static final String buildTable(@NotNull String vid) {
        return "veh.data.stream." + vid;
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
