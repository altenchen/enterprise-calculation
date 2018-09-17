package storm.handler.cusmade;

import com.google.common.collect.*;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.system.NoticeType;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author: xzp
 * @date: 2018-09-03
 * @description:
 * 车辆闲置处理, 用于判断车辆是否闲置.
 */
public final class VehicleIdleHandler {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(VehicleIdleHandler.class);

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    private static final int REDIS_DATABASE_INDEX = 6;

    private static final String IDLE_VEHICLE_REDIS_KEY = "vehCache.qy.idle";

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    /**
     * 车辆最新实时数据接收时间
     */
    private final Map<String, Long> vehiclePlatformReceiveTime = Maps.newHashMap();

    /**
     * 车辆闲置开始通知缓存
     */
    private final Map<String, ImmutableMap<String, String>> vehicleIdleNoticeCache = Maps.newHashMap();

    /**
     * 车辆闲置阈值, 默认1天
     */
    private long idleTimeoutMillisecond = TimeUnit.DAYS.toMillis(1);

    @Contract(pure = true)
    public long getIdleTimeoutMillisecond() {
        return idleTimeoutMillisecond;
    }

    public void setIdleTimeoutMillisecond(
        final long idleTimeoutMillisecond) {

        this.idleTimeoutMillisecond = idleTimeoutMillisecond;
    }

    public void initIdleNotice(
        @NotNull final String vid,
        @NotNull final ImmutableMap<String, String> startNotice) {

        vehicleIdleNoticeCache.putIfAbsent(vid, startNotice);
    }

    public ImmutableMap<String, String> updatePlatformReceiveTime(
        @NotNull final String vid,
        final long platformReceiveTime) {

        vehiclePlatformReceiveTime.compute(
            vid,
            (key, oldValue) -> {
                if(null != oldValue) {
                    return Math.max(oldValue, platformReceiveTime);
                }
                return platformReceiveTime;
            }
        );

        if (vehicleIdleNoticeCache.containsKey(vid)) {

            final long currentTimeMillis = System.currentTimeMillis();
            final Long latestPlatformReceiveTime = vehiclePlatformReceiveTime.get(vid);

            if (currentTimeMillis - latestPlatformReceiveTime <= idleTimeoutMillisecond) {

                final ImmutableMap<String, String> startNotice = vehicleIdleNoticeCache.remove(vid);

                final ImmutableMap<String, String> endNotice = buildEndNotice(
                    startNotice,
                    vid,
                    currentTimeMillis,
                    latestPlatformReceiveTime,
                    idleTimeoutMillisecond);


                JEDIS_POOL_UTILS.useResource(jedis -> {

                    jedis.select(REDIS_DATABASE_INDEX);
                    jedis.hdel(IDLE_VEHICLE_REDIS_KEY, vid);
                });

                return new ImmutableMap.Builder<String, String>()
                    .put(vid, JSON_UTILS.toJson(endNotice))
                    .build();
            }
        }

        return ImmutableMap.of();
    }

    @NotNull
    public ImmutableMap<String, String> computeNotice() {

        final Map<String, String> startNoticeList = Maps.newHashMap();
        final Map<String, String> endNoticeList = Maps.newHashMap();

        final long currentTimeMillis = System.currentTimeMillis();
        final long idleTimeoutMillisecond = this.idleTimeoutMillisecond;

        vehiclePlatformReceiveTime.forEach((final String vid, final Long serverReceiveTime)->{

            if(currentTimeMillis - serverReceiveTime > idleTimeoutMillisecond) {

                if(!vehicleIdleNoticeCache.containsKey(vid)) {

                    final ImmutableMap<String, String> startNotice = buildStartNotice(
                        vid,
                        currentTimeMillis,
                        serverReceiveTime,
                        idleTimeoutMillisecond);

                    vehicleIdleNoticeCache.put(vid, startNotice);

                    startNoticeList.put(
                        vid,
                        JSON_UTILS.toJson(startNotice));
                }

            } else {

                if (vehicleIdleNoticeCache.containsKey(vid)) {

                    final ImmutableMap<String, String> startNotice = vehicleIdleNoticeCache.remove(vid);

                    final ImmutableMap<String, String> endNotice = buildEndNotice(
                        startNotice,
                        vid,
                        currentTimeMillis,
                        serverReceiveTime,
                        idleTimeoutMillisecond);

                    endNoticeList.put(
                        vid,
                        JSON_UTILS.toJson(endNotice));
                }
            }
        });

        JEDIS_POOL_UTILS.useResource(jedis -> {

            jedis.select(REDIS_DATABASE_INDEX);

            endNoticeList.keySet().forEach(vid -> jedis.hdel(IDLE_VEHICLE_REDIS_KEY, vid));

            startNoticeList.forEach((vid, json)->jedis.hset(IDLE_VEHICLE_REDIS_KEY, vid, json));
        });

        return new ImmutableMap.Builder<String, String>()
            .putAll(startNoticeList)
            .putAll(endNoticeList)
            .build();
    }

    @NotNull
    private ImmutableMap<String, String> buildEndNotice(
        @NotNull final ImmutableMap<String, String> startNotice,
        @NotNull final String vid,
        final long currentTimeMillis,
        final Long serverReceiveTime,
        final long idleTimeoutMillisecond) {

        final String noticeTime = DataUtils.buildFormatTime(currentTimeMillis);
        final String idleTimeout = String.valueOf(idleTimeoutMillisecond);
        final String totalMileage = getTotalMileageString(vid);

        final Map<String, String> endNotice = Maps.newTreeMap();
        endNotice.putAll(startNotice);
        endNotice.put("eNoticeTime", noticeTime);
        endNotice.put("eTimeout", idleTimeout);
        endNotice.put("status", String.valueOf(3));
        endNotice.put("etime", DataUtils.buildFormatTime(serverReceiveTime));
        endNotice.put("emileage", totalMileage);

        endNotice.put("mileage", totalMileage);
        endNotice.put("noticetime", noticeTime);

        return ImmutableMap.copyOf(endNotice);
    }

    @NotNull
    private ImmutableMap<String, String> buildStartNotice(
        @NotNull final String vid,
        final long currentTimeMillis,
        final Long serverReceiveTime,
        final long idleTimeoutMillisecond) {

        final String noticeTime = DataUtils.buildFormatTime(currentTimeMillis);
        final String idleTimeout = String.valueOf(idleTimeoutMillisecond);
        final String totalMileage = getTotalMileageString(vid);

        final Map<String, String> startNotice = Maps.newTreeMap();
        startNotice.put("msgId", UUID.randomUUID().toString());
        startNotice.put("msgType", NoticeType.IDLE_VEH);
        startNotice.put("sNoticeTime", noticeTime);
        startNotice.put("sTimeout", idleTimeout);
        startNotice.put("vid", vid);
        startNotice.put("status", String.valueOf(1));
        startNotice.put("stime", DataUtils.buildFormatTime(serverReceiveTime));
        startNotice.put("smileage", totalMileage);
        startNotice.put("offlineMillisecondsThreshold", idleTimeout);

        return ImmutableMap.copyOf(startNotice);
    }

    @NotNull
    private static String getTotalMileageString(
        @NotNull final String vid) {

        try {
            return VEHICLE_CACHE.getTotalMileageString(vid, "");
        } catch (final ExecutionException e) {
            LOG.warn("获取累计里程缓存异常", e);
            return "";
        }
    }
}
