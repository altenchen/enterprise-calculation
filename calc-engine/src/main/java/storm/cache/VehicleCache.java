package storm.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;
import storm.constant.RedisConstant;
import storm.system.DataKey;
import storm.util.GsonUtils;
import storm.util.JedisPoolUtils;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 车辆数据缓存, 缓存了车辆的部分数据的最后有效值.
 *
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
@SuppressWarnings("unused")
public final class VehicleCache {

    private static final Logger logger = LoggerFactory.getLogger(VehicleCache.class);

    public static final int REDIS_DB_INDEX = 6;
    public static final String VALUE_TIME_KEY = "time";
    public static final String VALUE_DATA_KEY = "data";
    public static final String TOTAL_MILEAGE_FIELD = "useful" + DataKey._2202_TOTAL_MILEAGE;

    private static final GsonUtils GSON_UTILS = GsonUtils.getInstance();

    private static final VehicleCache INSTANCE = new VehicleCache();

    @Contract(pure = true)
    public static VehicleCache getInstance() {
        return INSTANCE;
    }

    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    @Contract(pure = true)
    @NotNull
    public static String buildRedisKey(@NotNull final String vid) {
        return "vehCache." + vid;
    }

    // region 定义缓存

    /**
     * 缓存结构: <vid, <field, <key, value>>>
     */
    private final LoadingCache<String, LoadingCache<String, ImmutableMap<String, String>>> cache =
        CacheBuilder
            .newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<String, LoadingCache<String, ImmutableMap<String, String>>>() {

                @Override
                public Map<String, LoadingCache<String, ImmutableMap<String, String>>> loadAll(
                    @NotNull final Iterable<? extends String> keys) {

                    final Map<String, LoadingCache<String, ImmutableMap<String, String>>> result = new TreeMap<>();

                    for (String key : keys) {
                        final String redisKey = buildRedisKey(key);
                        final LoadingCache<String, ImmutableMap<String, String>> map = loadHash(redisKey);
                        result.put(key, map);
                    }

                    return result;
                }

                @NotNull
                @Override
                public LoadingCache<String, ImmutableMap<String, String>> load(
                    @NotNull final String key) {
                    final String redisKey = buildRedisKey(key);
                    return loadHash(redisKey);
                }
            });

    @NotNull
    private LoadingCache<String, ImmutableMap<String, String>> loadHash(
        @NotNull final String redisKey) {

        return CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<String, ImmutableMap<String, String>>() {

                @NotNull
                @Override
                public Map<String, ImmutableMap<String, String>> loadAll(
                    @NotNull final Iterable<? extends String> fields)
                    throws JedisException, JsonIOException {

                    return loadFields(redisKey, fields);
                }

                @NotNull
                @Override
                public ImmutableMap<String, String> load(
                    @NotNull final String field)
                    throws JedisException, JsonIOException {
                    return loadField(redisKey, field);
                }
            });
    }

    // endregion 定义缓存

    // region 加载缓存

    @NotNull
    private Map<String, ImmutableMap<String, String>> loadFields(
        @NotNull final String key,
        @NotNull final Iterable<? extends String> fields)
        throws JedisException {

        return JEDIS_POOL_UTILS.useResource(jedis -> {

            final Map<String, ImmutableMap<String, String>> result = new TreeMap<>();

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                logger.trace("批量加载缓存[{}]{}", key, fields);

                final Map<String, String> jsons = jedis.hgetAll(key);

                for (String field : fields) {
                    if (!jsons.containsKey(field)) {
                        continue;
                    }

                    final String json = jsons.get(field);

                    try {
                        final Map<String, String> map =
                            GSON_UTILS.fromJson(
                                json,
                                TREE_MAP_STRING_STRING_TYPE
                            );

                        result.put(
                            field,
                            map == null ?
                                ImmutableMap.of()
                                : new ImmutableMap.Builder<String, String>().putAll(map).build());
                    } catch (JsonSyntaxException e) {
                        logger.warn("错误的数据格式Redis[{}][{}][{}]->{}", REDIS_DB_INDEX, key, field, json);
                    }
                }
            }

            for (String field : fields) {
                if (!result.containsKey(field)) {
                    result.put(field, ImmutableMap.of());
                }
            }
            return result;
        });
    }

    @NotNull
    private ImmutableMap<String, String> loadField(
        @NotNull final String key,
        @NotNull final String field)
        throws JedisException {

        return JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                logger.trace("单独加载缓存[{}][{}]", key, field);

                final String json = jedis.hget(key, field);

                try {
                    final Map<String, String> map = GSON_UTILS.fromJson(
                        json,
                        TREE_MAP_STRING_STRING_TYPE
                    );

                    return null == map ?
                        ImmutableMap.of()
                        : new ImmutableMap.Builder<String, String>().putAll(map).build();
                } catch (JsonSyntaxException e) {
                    logger.warn("错误的数据格式Redis[{}][{}][{}]->{}", REDIS_DB_INDEX, key, field, json);
                }
            }
            return ImmutableMap.of();
        });
    }

    // endregion 加载缓存

    // region 获取缓存

    @NotNull
    public ImmutableMap<String, String> getField(
        @NotNull final String vid,
        @NotNull final String field)
        throws ExecutionException {

        if (StringUtils.isBlank(vid) || StringUtils.isBlank(field)) {
            return ImmutableMap.of();
        }

        return cache.get(vid).get(field);
    }

    @NotNull
    public ImmutableMap<String, ImmutableMap<String, String>> getFields(
        @NotNull final String vid,
        @NotNull final Iterable<String> fields)
        throws ExecutionException {

        if (StringUtils.isBlank(vid)) {
            return ImmutableMap.of();
        }

        final Set<String> verifiedField = new HashSet<>();
        for (String field : fields) {
            if (StringUtils.isNotBlank(field)) {
                verifiedField.add(field);
            }
        }

        if (CollectionUtils.isEmpty(verifiedField)) {
            return ImmutableMap.of();
        }

        return cache.get(vid).getAll(verifiedField);
    }

    // endregion 获取缓存

    // region 更新缓存

    public void putField(
        @NotNull final String vid,
        @NotNull final String field,
        @NotNull final ImmutableMap<String, String> dictionary)
        throws JedisException, JsonParseException, ExecutionException {

        if (StringUtils.isBlank(vid) || StringUtils.isBlank(field)) {
            return;
        }

        final LoadingCache<String, ImmutableMap<String, String>> table = cache.get(vid);

        JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                logger.trace("单独更新缓存[{}][{}]", redisKey, field);

                if (MapUtils.isEmpty(dictionary)) {
                    jedis.hdel(redisKey, field);
                    table.invalidate(field);
                    return;
                }

                final String json = GSON_UTILS.toJson(dictionary);

                jedis.hset(redisKey, field, json);
                table.put(field, dictionary);
            }
        });
    }

    public void putFields(
        @Nullable final String vid,
        @Nullable final ImmutableMap<String, ImmutableMap<String, String>> dictionaries)
        throws JedisException, JsonParseException, ExecutionException {

        if (StringUtils.isBlank(vid) || MapUtils.isEmpty(dictionaries)) {
            return;
        }

        final LoadingCache<String, ImmutableMap<String, String>> table = cache.get(vid);

        JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                logger.trace("批量更新缓存[{}][{}]", redisKey, dictionaries.keySet());

                for (String field : dictionaries.keySet()) {

                    final ImmutableMap<String, String> dictionary = dictionaries.get(field);

                    if (MapUtils.isEmpty(dictionary)) {
                        jedis.hdel(redisKey, field);
                        table.invalidate(field);
                        continue;
                    }

                    final String json = GSON_UTILS.toJson(dictionary);

                    jedis.hset(redisKey, field, json);
                    table.put(field, dictionary);
                }
            }
        });
    }

    // endregion 更新缓存

    // region 删除缓存

    public void delField(
        @Nullable final String vid,
        @Nullable final String field)
        throws ExecutionException {

        if (StringUtils.isBlank(vid) || StringUtils.isBlank(field)) {
            return;
        }

        final LoadingCache<String, ImmutableMap<String, String>> table = cache.get(vid);

        JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                logger.trace("单独删除缓存[{}][{}]", redisKey, field);

                jedis.hdel(redisKey, field);
                table.invalidate(field);
            }
        });
    }

    public void delFields(
        @Nullable final String vid) {

        if (StringUtils.isBlank(vid)) {
            return;
        }

        JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                logger.trace("批量删除缓存[{}]", redisKey);

                jedis.del(redisKey);
                cache.invalidate(vid);
            }
        });
    }

    public void delFields(
        @Nullable final String vid,
        @Nullable final Iterable<String> fields)
        throws ExecutionException {

        if (StringUtils.isBlank(vid) || null == fields || !fields.iterator().hasNext()) {
            return;
        }

        final LoadingCache<String, ImmutableMap<String, String>> table = cache.get(vid);

        JEDIS_POOL_UTILS.useResource(jedis -> {

            final String select = jedis.select(REDIS_DB_INDEX);
            if (!RedisConstant.Select.OK.equals(select)) {

                logger.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                logger.trace("批量删除缓存[{}][{}]", redisKey, fields);

                for (String field : fields) {

                    jedis.hdel(redisKey, field);
                    table.invalidate(field);
                }
            }
        });
    }

    // endregion 删除缓存

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

    // region 封装

    @NotNull
    public String getTotalMileageString(
        @NotNull String vid,
        @NotNull String defaultValue)
        throws ExecutionException {

        final ImmutableMap<String, String> totalMileageCache =
            getField(
                vid,
                VehicleCache.TOTAL_MILEAGE_FIELD);
        return StringUtils.defaultIfEmpty(
            totalMileageCache.get(
                VehicleCache.VALUE_DATA_KEY),
            defaultValue);
    }

    // endregion
}
