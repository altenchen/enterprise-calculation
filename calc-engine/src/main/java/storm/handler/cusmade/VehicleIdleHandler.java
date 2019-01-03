package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import storm.cache.VehicleCache;
import storm.extension.ObjectExtension;
import storm.system.NoticeType;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    /**
     * 车辆最新实时数据接收时间
     */
    private final Map<String, Long> vehiclePlatformReceiveTime = Maps.newHashMap();

    /**
     * 车辆闲置状态
     * -1   闲置结束
     * 0    未知状态
     * 1    闲置开始
     */
    private final Map<String, Integer> vehicleStatus = Maps.newHashMap();

    private static final Integer STATUS_START = 1;
    private static final Integer STATUS_UNKNOWN = 0;
    private static final Integer STATUS_STOP = -1;

    /**
     * 车辆闲置阈值, 默认1天
     */
    private long idleTimeoutMillisecond = TimeUnit.DAYS.toMillis(1);

    @Contract(pure = true)
    public long getIdleTimeoutMillisecond() {
        return idleTimeoutMillisecond;
    }

    public void setIdleTimeoutMillisecond(final long idleTimeoutMillisecond) {
        this.idleTimeoutMillisecond = idleTimeoutMillisecond;
    }

    /**
     * 初始化平台接收时间, 只会初始化一次.
     * 如果有持久化的闲置开始通知, 则生成闲置结束通知
     * 更新车辆状态为闲置结束
     * @param vehicleId 车辆标识
     * @param platformReceiveTime 平台接收时间
     * @return 闲置结束通知或者空字典
     */
    public ImmutableMap<String, String> initPlatformReceiveTime(
        @NotNull final String vehicleId,
        final long platformReceiveTime) {

        if (vehiclePlatformReceiveTime.containsKey(vehicleId)) {
            return ImmutableMap.of();
        } else {
            vehiclePlatformReceiveTime.put(vehicleId, platformReceiveTime);
            return computeEndNotice(vehicleId, platformReceiveTime);
        }
    }

    /**
     * 更新平台接收时间
     * 如果有持久化的闲置开始通知, 则生成闲置结束通知
     * 更新车辆状态为闲置结束
     * @param vehicleId 车辆标识
     * @param platformReceiveTime 平台接收时间
     * @return 闲置结束通知或者空字典
     */
    public ImmutableMap<String, String> updatePlatformReceiveTime(
        @NotNull final String vehicleId,
        final long platformReceiveTime) {

        final Long latestPlatformReceiveTime = vehiclePlatformReceiveTime.compute(
            vehicleId,
            (key, oldValue) -> {
                if(null != oldValue) {
                    return Math.max(oldValue, platformReceiveTime);
                }
                return platformReceiveTime;
            }
        );

        return computeEndNotice(vehicleId, latestPlatformReceiveTime);
    }

    private ImmutableMap<String, String> computeEndNotice(
        final @NotNull String vehicleId,
        final Long latestPlatformReceiveTime) {

        final Integer status = vehicleStatus.getOrDefault(vehicleId, STATUS_UNKNOWN);
        if(!STATUS_STOP.equals(status)) {

            final ImmutableMap<String, String> notice = JEDIS_POOL_UTILS.useResource(jedis -> {

                jedis.select(REDIS_DATABASE_INDEX);

                final ImmutableMap<String, String> startNotice = getNotice(jedis, vehicleId);
                if (MapUtils.isNotEmpty(startNotice)) {

                    final long currentTimeMillis = System.currentTimeMillis();

                    final ImmutableMap<String, String> endNotice = buildEndNotice(
                        startNotice,
                        vehicleId,
                        currentTimeMillis,
                        latestPlatformReceiveTime,
                        idleTimeoutMillisecond);
                    jedis.hdel(IDLE_VEHICLE_REDIS_KEY, vehicleId);

                    final String json = JSON_UTILS.toJson(endNotice);
                    LOG.info("VID:{} 车辆闲置结束. {}", vehicleId, json);

                    return ImmutableMap.of(vehicleId, json);
                } else {
                    return ImmutableMap.of();
                }
            });
            vehicleStatus.put(vehicleId, STATUS_STOP);
            return notice;
        } else {
            return ImmutableMap.of();
        }
    }

    /**
     * 计算闲置开始通知
     * @return 闲置开始通知字典 <vid, json>
     */
    @NotNull
    public ImmutableMap<String, String> computeStartNotice() {

        final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();

        final long currentTimeMillis = System.currentTimeMillis();
        final long idleTimeoutMillisecond = this.idleTimeoutMillisecond;

        JEDIS_POOL_UTILS.useResource(jedis -> {

            jedis.select(REDIS_DATABASE_INDEX);

            final ImmutableSet<String> idleVehicleIds = getIdleVehicleIds(jedis);

            vehiclePlatformReceiveTime.forEach((final String vehicleId, final Long serverReceiveTime)->{

                if(currentTimeMillis - serverReceiveTime > idleTimeoutMillisecond) {
                    final Integer status = vehicleStatus.getOrDefault(vehicleId, STATUS_UNKNOWN);
                    if (!STATUS_START.equals(status)) {
                        if (!idleVehicleIds.contains(vehicleId)) {
                            final ImmutableMap<String, String> startNotice = buildStartNotice(
                                vehicleId,
                                currentTimeMillis,
                                serverReceiveTime,
                                idleTimeoutMillisecond);
                            final String json = JSON_UTILS.toJson(startNotice);
                            LOG.info("VID:{} 车辆闲置开始. {}", vehicleId, json);

                            jedis.hset(IDLE_VEHICLE_REDIS_KEY, vehicleId, json);

                            builder.put(vehicleId,json);
                        }
                        vehicleStatus.put(vehicleId, STATUS_START);
                    }
                }
            });
        });

        return builder.build();
    }

    @NotNull
    private ImmutableMap<String, String> getNotice(
        @NotNull final Jedis jedis,
        @NotNull final String vid) {
        final String json = jedis.hget(IDLE_VEHICLE_REDIS_KEY, vid);
        if (StringUtils.isBlank(json)) {
            return ImmutableMap.of();
        } else {
            final TreeMap<String, String> notice = JSON_UTILS.fromJson(
                json,
                TREE_MAP_STRING_STRING_TYPE);
            return ImmutableMap.copyOf(notice);
        }
    }

    @NotNull
    private ImmutableSet<String> getIdleVehicleIds(
        @NotNull final Jedis jedis) {

        final Set<String> vehicleIds = jedis.hkeys(IDLE_VEHICLE_REDIS_KEY);
        return ImmutableSet.copyOf(
            ObjectExtension.defaultIfNull(
                vehicleIds,
                ImmutableSet::of
            )
        );
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
            LOG.warn("VID:" + vid + " 获取累计里程缓存异常", e);
            return "";
        }
    }
}
