package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;
import storm.constant.RedisConstant;
import storm.util.JedisPoolUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 车辆数据缓存, 缓存了车辆的部分数据的最后有效值.
 *
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
public final class VehicleCache {

    private static final Logger logger = LoggerFactory.getLogger(VehicleCache.class);

    private static final VehicleCache INSTANCE = new VehicleCache();

    @Contract(pure = true)
    public static VehicleCache getInstance() {
        return INSTANCE;
    }

    private static final int REDIS_DB_INDEX = 6;

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    @NotNull
    private static final String buildKey(@NotNull final String vid) {
        return "vehCache." + vid;
    }

    /**
     * <vid, <field, value>>
     */
    private final Cache<String, Cache<String, Optional<String>>> cache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.DAYS)
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build(new CacheLoader<String, Cache<String, Optional<String>>>() {
            @NotNull
            @Override
            public Cache<String, Optional<String>> load(@NotNull final String vid) {
                final String key = buildKey(vid);
                return loadVehicleCache(key);
            }
        });

    @NotNull
    private Cache<String, Optional<String>> loadVehicleCache(@NotNull final String key) {
        return CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Optional<String>>() {

                @Nullable
                @Override
                public Optional<String> load(@NotNull final String field) throws JedisException {
                    return loadVehicleCache(key, field);
                }

                @Override
                public Map<String, Optional<String>> loadAll(Iterable<? extends String> keys) throws Exception {
                    return JEDIS_POOL_UTILS.useResource((jedis) -> {
                        Map<String, Optional<String>> result = new HashMap<>();
                        final String select = jedis.select(REDIS_DB_INDEX);
                        //noinspection AlibabaUndefineMagicConstant
                        if (!RedisConstant.Select.OK.name().equals(select)) {
                            logger.warn("切换车辆缓存库失败");
                            return result;
                        }

                        logger.warn("批量加载缓存[{}]", key);
                        final Map<String, String> dic = jedis.hgetAll(key);
                        if (MapUtils.isNotEmpty(dic)) {
                            for (Map.Entry<String, String> entry : dic.entrySet()) {
                                result.put(
                                    entry.getKey(),
                                    Optional.ofNullable(
                                        entry.getValue()));
                            }
                        }
                        return result;
                    });
                }
            });
    }

    @NotNull
    private Optional<String> loadVehicleCache(@NotNull final String key, @NotNull final String field)
        throws JedisException {

        return Optional.ofNullable(JEDIS_POOL_UTILS.useResource(jedis -> {
            final String select = jedis.select(REDIS_DB_INDEX);
            //noinspection AlibabaUndefineMagicConstant
            if (!RedisConstant.Select.OK.name().equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                logger.trace("单独加载缓存[{}][{}]", key, field);
                final String value = jedis.hget(key, field);
                return value;
            }
            return null;
        }));
    }

    private Cache<String, Optional<String>> getVehicleCache(String vid)
        throws ExecutionException {

        return cache.get(
            vid,
            () -> {
                final String key = buildKey(vid);
                return loadVehicleCache(key);
            });
    }

    public String getVehicleCache(String vid, String field)
        throws ExecutionException {

        final Cache<String, Optional<String>> cache = getVehicleCache(vid);
        cache.asMap();
        return cache.get(
            field,
            () -> {
                final String key = buildKey(vid);
                return loadVehicleCache(key, field);
            }).orElseGet(() -> null);
    }
}
