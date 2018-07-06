package storm.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.ObjectUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;
import storm.constant.RedisConstant;
import storm.util.JedisPoolUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
     * 缓存结构: <vid, <field, <key, value>>>
     */
    private final LoadingCache<String, LoadingCache<String, Map<String, String>>> cache =
        CacheBuilder
            .newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<String, LoadingCache<String, Map<String, String>>>() {
                @NotNull
                @Override
                public LoadingCache<String, Map<String, String>> load(@NotNull final String vid) {
                    final String key = buildKey(vid);
                    return loadVehicleCache(key);
                }
            });

    // region 加载缓存

    @NotNull
    private LoadingCache<String, Map<String, String>> loadVehicleCache(
        @NotNull final String key) {

        return CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Map<String, String>>() {

                @NotNull
                @Override
                public Map<String, String> load(
                    @NotNull final String field)
                    throws JedisException {
                    return loadVehicleCache(key, field);
                }

                @NotNull
                @Override
                public Map<String, Map<String, String>> loadAll(
                    @NotNull final Iterable<? extends String> fields)
                    throws JedisException {

                    return loadVehicleCache(key, fields);
                }
            });
    }

    @NotNull
    private Map<String, String> loadVehicleCache(
        @NotNull final String key,
        @NotNull final String field)
        throws JedisException {

        return (Map<String, String>) ObjectUtils.defaultIfNull(JEDIS_POOL_UTILS.useResource(jedis -> {
            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.name().equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                logger.trace("单独加载缓存[{}][{}]", key, field);

                final String json = jedis.hget(key, field);
                final Gson gson = new Gson();

                try {
                    final Map<String, String> map = gson.fromJson(
                        json,
                        new TypeToken<HashMap<String, String>>() {
                        }.getType());

                    return map;
                } catch (JsonSyntaxException e) {
                    logger.warn("错误的数据格式Redis[{}][{}][{}]->{}", REDIS_DB_INDEX, key, field, json);
                }
            }
            return null;
        }), new HashMap<>());
    }

    @NotNull
    private Map<String, Map<String, String>> loadVehicleCache(
        @NotNull final String key,
        @NotNull final Iterable<? extends String> fields)
        throws JedisException {

        return JEDIS_POOL_UTILS.useResource(jedis -> {
            final Map<String, Map<String, String>> result = new HashMap<>();

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.name().equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                logger.trace("批量加载缓存[{}]{}", key, fields);

                final Map<String, String> jsons = jedis.hgetAll(key);
                final Gson gson = new Gson();

                for (Map.Entry<String, String> entry : jsons.entrySet()) {
                    final String field = entry.getKey();
                    final String json = entry.getValue();

                    try {
                        final Map<String, String> map = (Map<String, String>) ObjectUtils.defaultIfNull(
                            gson.fromJson(
                                json,
                                new TypeToken<HashMap<String, String>>() {
                                }.getType()
                            ),
                            new HashMap<>()
                        );

                        result.put(field, map);
                    } catch (JsonSyntaxException e) {
                        logger.warn("错误的数据格式Redis[{}][{}][{}]->{}", REDIS_DB_INDEX, key, field, json);
                    }
                }
            }
            for (String field : fields) {
                if(!result.containsKey(field)) {
                    result.put(field, new HashMap<>());
                }
            }
            return result;
        });
    }

    // endregion 加载缓存

    // region 获取缓存

    @NotNull
    public Map<String, String> getVehicleCache(
        @NotNull final String vid,
        @NotNull final String field)
        throws ExecutionException {

        return cache.get(vid).get(field);
    }

    @NotNull
    public Map<String, Map<String, String>> getVehicleCache(
        @NotNull final String vid,
        @NotNull final Iterable<String> fields)
        throws ExecutionException {

        return cache.get(vid).getAll(fields);
    }

    @NotNull
    public Map<String, Map<String, String>> getVehicleCache(
        @NotNull final String vid,
        @NotNull final String... fields)
        throws ExecutionException {

        return cache.get(vid).getAll(Stream.of(fields).collect(Collectors.toSet()));
    }

    // endregion 获取缓存

    // region 清除缓存

    public void invalidateAll() {

        cache.invalidateAll();
    }

    public void invalidateAll(
        @NotNull final Iterable<String> vids) {

        cache.invalidateAll(vids);
    }

    public void invalidate(
        @NotNull final String vid) {

        cache.invalidate(vid);
    }

    public void invalidateAll(
        @NotNull final String vid)
        throws ExecutionException {

        cache.get(vid).invalidateAll();
    }

    public void invalidateAll(
        @NotNull final String vid,
        @NotNull final Iterable<String> fields)
        throws ExecutionException {

        cache.get(vid).invalidateAll(fields);
    }

    public void invalidate(
        @NotNull final String vid,
        @NotNull final String field)
        throws ExecutionException {

        cache.get(vid).invalidate(field);
    }

    // endregion 清除缓存
}
