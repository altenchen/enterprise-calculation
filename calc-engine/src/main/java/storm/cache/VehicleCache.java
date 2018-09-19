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
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;
import storm.constant.FormatConstant;
import storm.constant.RedisConstant;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.util.DataUtils;
import storm.util.JsonUtils;
import storm.util.JedisPoolUtils;
import storm.util.ParamsRedisUtil;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 车辆数据缓存, 缓存了车辆的部分数据的最后有效值.
 *
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
@SuppressWarnings({"unused", "UnstableApiUsage"})
public final class VehicleCache {

    private static final Logger LOG = LoggerFactory.getLogger(VehicleCache.class);

    private static final VehicleCache INSTANCE = new VehicleCache();

    @Contract(pure = true)
    public static VehicleCache getInstance() {
        return INSTANCE;
    }

    public static final int REDIS_DB_INDEX = 6;
    public static final String VALUE_TIME_KEY = "time";
    public static final String VALUE_DATA_KEY = "data";
    public static final String TOTAL_MILEAGE_FIELD = "useful" + DataKey._2202_TOTAL_MILEAGE;
    public static final String ORIENTATION_FIELD = "useful" + DataKey._2501_ORIENTATION;
    public static final String LONGITUDE_FIELD = "useful" + DataKey._2502_LONGITUDE;
    public static final String LATITUDE_FIELD = "useful" + DataKey._2503_LATITUDE;
    public static final String JILI_LOCK = "JILI_LOCK";

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();

    @Contract(pure = true)
    @NotNull
    public static String buildRedisKey(@NotNull final String vid) {
        return "vehCache." + vid;
    }

    private VehicleCache() {

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

                LOG.warn("切换车辆缓存库失败");
            } else {

                LOG.trace("批量加载缓存[{}]{}", key, fields);

                final Map<String, String> jsons = jedis.hgetAll(key);

                for (String field : fields) {
                    if (!jsons.containsKey(field)) {
                        continue;
                    }

                    final String json = jsons.get(field);

                    try {
                        final Map<String, String> map =
                            JSON_UTILS.fromJson(
                                json,
                                TREE_MAP_STRING_STRING_TYPE
                            );

                        result.put(
                            field,
                            map == null ?
                                ImmutableMap.of()
                                : new ImmutableMap.Builder<String, String>().putAll(map).build());
                    } catch (JsonSyntaxException e) {
                        LOG.warn("错误的数据格式Redis[{}][{}][{}]->{}", REDIS_DB_INDEX, key, field, json);
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

                LOG.warn("切换车辆缓存库失败");
            } else {

                LOG.trace("单独加载缓存[{}][{}]", key, field);

                final String json = jedis.hget(key, field);

                try {
                    final Map<String, String> map = JSON_UTILS.fromJson(
                        json,
                        TREE_MAP_STRING_STRING_TYPE
                    );

                    return null == map ?
                        ImmutableMap.of()
                        : new ImmutableMap.Builder<String, String>().putAll(map).build();
                } catch (JsonSyntaxException e) {
                    LOG.warn("错误的数据格式Redis[{}][{}][{}]->{}", REDIS_DB_INDEX, key, field, json);
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
    public ImmutableMap<String, String> getField(
        @NotNull final String vid,
        @NotNull final String field,
        @NotNull Supplier<@NotNull ImmutableMap<String, String>> defaultValue) {

        try {
            return getField(vid, field);
        } catch (ExecutionException e) {
            return defaultValue.get();
        }
    }

    @NotNull
    public ImmutableMap<String, String> getField(
        @NotNull final String vid,
        @NotNull final String field,
        @NotNull Function<@NotNull ExecutionException, @NotNull ImmutableMap<String, String>> defaultValue) {

        try {
            return getField(vid, field);
        } catch (ExecutionException e) {
            return defaultValue.apply(e);
        }
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

                LOG.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                LOG.trace("单独更新缓存[{}][{}]", redisKey, field);

                if (MapUtils.isEmpty(dictionary)) {
                    jedis.hdel(redisKey, field);
                    table.invalidate(field);
                    return;
                }

                final String json = JSON_UTILS.toJson(dictionary);

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

                LOG.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                LOG.trace("批量更新缓存[{}][{}]", redisKey, dictionaries.keySet());

                for (String field : dictionaries.keySet()) {

                    final ImmutableMap<String, String> dictionary = dictionaries.get(field);

                    if (MapUtils.isEmpty(dictionary)) {
                        jedis.hdel(redisKey, field);
                        table.invalidate(field);
                        continue;
                    }

                    final String json = JSON_UTILS.toJson(dictionary);

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

                LOG.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                LOG.trace("单独删除缓存[{}][{}]", redisKey, field);

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

                LOG.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                LOG.trace("批量删除缓存[{}]", redisKey);

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

                LOG.warn("切换车辆缓存库失败");
            } else {

                final String redisKey = buildRedisKey(vid);

                LOG.trace("批量删除缓存[{}][{}]", redisKey, fields);

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

    /**
     * 通过来自 SUBMIT_REALTIME 的实时数据更新一些常用的数据缓存
     * @param data 来自 SUBMIT_REALTIME 的实时数据
     */
    public void updateUsefulCache(
        @NotNull final ImmutableMap<String, String> data) {

        final String prefix = data.get(DataKey.PREFIX);
        if(!CommandType.SUBMIT.equals(prefix)) {
            return;
        }

        final String cmd = data.get(DataKey.MESSAGE_TYPE);
        if(!CommandType.SUBMIT_REALTIME.equals(cmd)) {
            return;
        }

        final String vid = data.get(DataKey.VEHICLE_ID);
        if(StringUtils.isBlank(vid)) {
            return;
        }

        final String platformReceiveTime = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        if (!NumberUtils.isDigits(platformReceiveTime)) {
            return;
        }
        try {
            DataUtils.parseFormatTime(platformReceiveTime);
        } catch (ParseException e) {
            LOG.warn("时间解析异常", e);
            LOG.warn("无效的格式化平台接收时间:[{}][{}]", FormatConstant.DATE_FORMAT, platformReceiveTime);
            return;
        }

        // region 缓存累计里程有效值

        final String totalMileage = data.get(DataKey._2202_TOTAL_MILEAGE);
        updateUsefulTotalMileage(vid, platformReceiveTime, totalMileage);

        // endregion 缓存累计里程有效值

        // region 缓存GPS定位有效值

        final String orientationString = data.get(DataKey._2501_ORIENTATION);
        final String longitudeString = data.get(DataKey._2502_LONGITUDE);
        final String latitudeString = data.get(DataKey._2503_LATITUDE);

        updateUsefulLocation(vid, platformReceiveTime, orientationString, longitudeString, latitudeString);

        // endregion 缓存GPS定位有效值
    }

    @SuppressWarnings("AlibabaMethodTooLong")
    private void updateUsefulLocation(
        @NotNull final String vid,
        @NotNull final String platformReceiveTime,
        @Nullable final String orientationString,
        @Nullable final String longitudeString,
        @Nullable final String latitudeString) {

        if (!NumberUtils.isDigits(orientationString)
            || !NumberUtils.isDigits(longitudeString)
            || !NumberUtils.isDigits(latitudeString)) {
            return;
        }

        final int orientationValue = NumberUtils.toInt(orientationString);
        final int longitudeValue = NumberUtils.toInt(longitudeString);
        final int latitudeValue = NumberUtils.toInt(latitudeString);

        if (!DataUtils.isOrientationUseful(orientationValue)
            || !DataUtils.isOrientationLongitudeUseful(longitudeValue)
            || !DataUtils.isOrientationLatitudeUseful(latitudeValue)) {
            return;
        }

        final ImmutableMap<String, String> usefulOrientation;
        final ImmutableMap<String, String> usefulLongitude;
        final ImmutableMap<String, String> usefulLatitude;
        try {
            usefulOrientation = INSTANCE.getField(
                vid,
                VehicleCache.ORIENTATION_FIELD);
            usefulLongitude =
                INSTANCE.getField(
                    vid,
                    VehicleCache.LONGITUDE_FIELD);
            usefulLatitude =
                INSTANCE.getField(
                    vid,
                    VehicleCache.LATITUDE_FIELD);
        } catch (ExecutionException e) {
            LOG.warn("获取有效定位缓存异常", e);
            return;
        }

        if(MapUtils.isNotEmpty(usefulOrientation)) {
            final String oldOrientationTime = usefulOrientation.get(VehicleCache.VALUE_TIME_KEY);
            if (NumberUtils.isDigits(oldOrientationTime) && platformReceiveTime.compareTo(oldOrientationTime) <= 0) {
                return;
            }
        }
        if(MapUtils.isNotEmpty(usefulLongitude)) {
            final String oldLongitudeTime = usefulLongitude.get(VehicleCache.VALUE_TIME_KEY);
            if (NumberUtils.isDigits(oldLongitudeTime) && platformReceiveTime.compareTo(oldLongitudeTime) <= 0) {
                return;
            }
        }
        if(MapUtils.isNotEmpty(usefulLatitude)) {
            final String oldLatitudeTime = usefulLatitude.get(VehicleCache.VALUE_TIME_KEY);
            if (NumberUtils.isDigits(oldLatitudeTime) && platformReceiveTime.compareTo(oldLatitudeTime) <= 0) {
                return;
            }
        }

        // TODO: [使用滤波算法过滤漂移值](http://www.geek-workshop.com/thread-7694-1-1.html)

        final ImmutableMap<String, String> updateOrientation = new ImmutableMap.Builder<String, String>()
            .put(VehicleCache.VALUE_TIME_KEY, platformReceiveTime)
            .put(VehicleCache.VALUE_DATA_KEY, orientationString)
            .build();
        final ImmutableMap<String, String> updateLongitude = new ImmutableMap.Builder<String, String>()
            .put(VehicleCache.VALUE_TIME_KEY, platformReceiveTime)
            .put(VehicleCache.VALUE_DATA_KEY, longitudeString)
            .build();
        final ImmutableMap<String, String> updateLatitude = new ImmutableMap.Builder<String, String>()
            .put(VehicleCache.VALUE_TIME_KEY, platformReceiveTime)
            .put(VehicleCache.VALUE_DATA_KEY, latitudeString)
            .build();
        final ImmutableMap<String, ImmutableMap<String, String>> update = new ImmutableMap.Builder<String, ImmutableMap<String, String>>()
            .put(VehicleCache.ORIENTATION_FIELD, updateOrientation)
            .put(VehicleCache.LONGITUDE_FIELD, updateLongitude)
            .put(VehicleCache.LATITUDE_FIELD, updateLatitude)
            .build();

        try {
            INSTANCE.putFields(
                vid,
                update
            );
        } catch (final ExecutionException e) {
            LOG.warn("更新有效定位缓存异常", e);
        }
    }

    private void updateUsefulTotalMileage(
        @NotNull final String vid,
        @NotNull final String platformReceiveTime,
        @Nullable final String totalMileage) {

        if (!NumberUtils.isDigits(totalMileage)) {
            return;
        }

        final ImmutableMap<String, String> usefulTotalMileage;
        try {
            usefulTotalMileage =
                INSTANCE.getField(
                    vid,
                    VehicleCache.TOTAL_MILEAGE_FIELD);
        } catch (final ExecutionException e) {
            LOG.warn("获取有效累计里程缓存异常", e);
            return;
        }

        if (MapUtils.isNotEmpty(usefulTotalMileage)) {
            final String cacheTime = usefulTotalMileage.get(VehicleCache.VALUE_TIME_KEY);
            if (NumberUtils.isDigits(cacheTime) && platformReceiveTime.compareTo(cacheTime) <= 0) {
                return;
            }
        }

        // TODO: [使用滤波算法计算有效值](http://www.geek-workshop.com/thread-7694-1-1.html)

        final ImmutableMap<String, String> update = new ImmutableMap.Builder<String, String>()
            .put(VehicleCache.VALUE_TIME_KEY, platformReceiveTime)
            .put(VehicleCache.VALUE_DATA_KEY, totalMileage)
            .build();
        try {
            INSTANCE.putField(
                vid,
                VehicleCache.TOTAL_MILEAGE_FIELD,
                update);
        } catch (final ExecutionException e) {
            LOG.warn("更新有效累计里程缓存异常", e);
        }
    }

    // endregion

}
