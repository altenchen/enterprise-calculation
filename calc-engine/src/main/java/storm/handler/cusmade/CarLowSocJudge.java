package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import storm.constant.FormatConstant;
import storm.extension.ObjectExtension;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.tool.DelaySwitch;
import storm.util.ConfigUtils;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;
import storm.util.function.TeConsumer;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author 徐志鹏
 * SOC过低预警
 */
class CarLowSocJudge {

    private static final Logger LOG = LoggerFactory.getLogger(CarLowSocJudge.class);

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();
    private static final String NOTICE_STATUS_KEY = "status";
    private static final String NOTICE_STATUS_START = "1";
    private static final String NOTICE_STATUS_END = "3";

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final int REDIS_DATABASE_INDEX = 6;
    private static final String SOC_LOW_REDIS_KEY = "vehCache.qy.soc.notice";
    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

    @NotNull
    @Contract(" -> new")
    private static DelaySwitch buildDelaySwitch() {
        return new DelaySwitch(
            ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerTimeoutMillisecond(),
            ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerContinueCount(),
            ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerTimeoutMillisecond());
    }


    // region 状态表

    /**
     * 车辆状态缓存表 <vehicleId, DelaySwitch></>
     */
    private final Map<String, DelaySwitch> vehicleStatus = Maps.newHashMap();

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void syncDelaySwitchConfig() {
        vehicleStatus.forEach((vehicleId, delaySwitch)->{
            delaySwitch.setPositiveThreshold(ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerContinueCount());
            delaySwitch.setPositiveTimeout(ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerTimeoutMillisecond());
            delaySwitch.setNegativeThreshold(ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerContinueCount());
            delaySwitch.setNegativeTimeout(ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerTimeoutMillisecond());
        });
    }

    @NotNull
    private DelaySwitch ensureVehicleStatus(@NotNull final String vehicleId) {
        return vehicleStatus.computeIfAbsent(
            vehicleId,
            k -> JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(REDIS_DATABASE_INDEX);

                final String json = jedis.hget(SOC_LOW_REDIS_KEY, vehicleId);
                if (StringUtils.isNotBlank(json)) {
                    final ImmutableMap<String, String> startNotice = loadSocLowNoticeFromRedis(jedis, vehicleId);
                    final String status = startNotice.get(NOTICE_STATUS_KEY);
                    if(NOTICE_STATUS_START.equals(status)) {
                        return buildDelaySwitch().setSwitchStatus(true);
                    } else if(NOTICE_STATUS_END.equals(status)) {
                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中已结束的平台报警通知 {}", vehicleId, REDIS_DATABASE_INDEX, SOC_LOW_REDIS_KEY, json);
                        jedis.hdel(SOC_LOW_REDIS_KEY, vehicleId);
                    } else {
                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中状态为 {} 的异常平台报警通知 {}", vehicleId, REDIS_DATABASE_INDEX, SOC_LOW_REDIS_KEY, status, json);
                        jedis.hdel(SOC_LOW_REDIS_KEY, vehicleId);
                    }
                }
                return buildDelaySwitch().setSwitchStatus(false);
            })
        );
    }

    @NotNull
    private ImmutableMap<String, String> loadSocLowNoticeFromRedis(
        @NotNull final Jedis jedis,
        @NotNull final String vehicleId) {

        final String json = jedis.hget(SOC_LOW_REDIS_KEY, vehicleId);
        if (StringUtils.isNotBlank(json)) {
            return ImmutableMap.copyOf(
                ObjectExtension.defaultIfNull(
                    JSON_UTILS.fromJson(
                        json,
                        TREE_MAP_STRING_STRING_TYPE,
                        e -> {
                            LOG.warn("VID:{} REDIS DB:{} KEY:{} 中不是合法json的SOC过低通知 {}", vehicleId, REDIS_DATABASE_INDEX, SOC_LOW_REDIS_KEY, json);
                            return null;
                        }),
                    Maps::newTreeMap)
            );
        } else {
            return ImmutableMap.of();
        }
    }

    /**
     * 车辆通知缓存表 <vehicleId, partNotice>
     */
    private final Map<String, ImmutableMap<String, String>> vehicleNoticeCache = Maps.newHashMap();

    // endregion 状态表

    /**
     * @param data 车辆数据
     * @param processChargeCars 当soc过低开始被触发时, 回调该函数处理补电车事宜(车辆标识, 经度, 纬度)
     * @return 如果产生低电量通知, 则填充通知, 否则为空集合.
     */
    @SuppressWarnings("WeakerAccess")
    public String processFrame(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final TeConsumer<@NotNull String, @NotNull Double, @NotNull Double> processChargeCars) {

        final String vehicleId = data.get(DataKey.VEHICLE_ID);
        final String platformReceiverTimeString = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        final String socString = data.get(DataKey._7615_STATE_OF_CHARGE);

        //检查数据有效性
        if (StringUtils.isBlank(vehicleId)
            || !NumberUtils.isDigits(platformReceiverTimeString)
            || !NumberUtils.isDigits(socString)){
            return null;
        }

        final long platformReceiverTime;
        try {
            platformReceiverTime = DataUtils.parseFormatTime(platformReceiverTimeString);
        } catch (@NotNull final ParseException e) {
            LOG.warn("解析服务器时间异常", e);
            return null;
        }
        final int soc = Integer.parseInt(socString);

        // 检验SOC是否小于等于过低开始阈值
        if(soc <= ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerThreshold()) {
            final String[] result = new String[1];
            final DelaySwitch delaySwitch = ensureVehicleStatus(vehicleId);
            delaySwitch.positiveIncrease(
                platformReceiverTime,
                () -> socLowBeginReset(data, vehicleId, platformReceiverTimeString, soc),
                (count, timeout) -> {
                    socLowBeginOverflow(count, timeout, vehicleId, result);
                    processChargeCars(data, vehicleId, processChargeCars);
                }
            );
            return result[0];
        }
        // 检验SOC是否大于过低结束阈值
        else if(soc > ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerThreshold()) {
            final String[] result = new String[1];
            final DelaySwitch delaySwitch = ensureVehicleStatus(vehicleId);
            delaySwitch.negativeIncrease(
                platformReceiverTime,
                () -> socLowEndReset(data, vehicleId, platformReceiverTimeString, soc),
                (count, timeout) -> socLowEndOverflow(count, timeout, vehicleId, result)
            );
            return result[0];
        } else {
            return null;
        }
    }

    // region SOC过低开始

    private void socLowBeginReset(
        final @NotNull ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString,
        final int soc) {

        final String longitudeString = data.get(DataKey._2502_LONGITUDE);
        final String latitudeString = data.get(DataKey._2503_LATITUDE);
        @NotNull final String location = StringUtils.defaultString(
            DataUtils.buildLocation(
                longitudeString,
                latitudeString
            )
        );

        vehicleNoticeCache.put(
            vehicleId,
            new ImmutableMap.Builder<String, String>()
                .put("msgType", NoticeType.SOC_LOW_NOTICE)
                .put("msgId", UUID.randomUUID().toString())
                .put("vid", vehicleId)
                .put("stime", platformReceiverTimeString)
                .put("location", location)
                .put("slocation", location)
                .put("sthreshold", String.valueOf(ConfigUtils.getSysDefine().getNoticeSocLowBeginTriggerThreshold()))
                .put("ssoc", String.valueOf(soc))
                // 兼容性处理, 暂留
                .put("lowSocThreshold", String.valueOf(ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerThreshold()))
                .build()
        );

        LOG.trace("VID:{} SOC过低开始首帧缓存初始化", vehicleId);
    }

    private void socLowBeginOverflow(
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final String[] result){

        final long currentTimeMillis = System.currentTimeMillis();
        final String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

        final Map<String, String> socLowStartNotice = Maps.newHashMap(
            vehicleNoticeCache.get(vehicleId)
        );
        socLowStartNotice.put("status", NOTICE_STATUS_START);
        socLowStartNotice.put("scontinue", String.valueOf(count));
        socLowStartNotice.put("slazy", String.valueOf(timeout));
        socLowStartNotice.put("noticeTime", noticeTime);

        final String json = JSON_UTILS.toJson(socLowStartNotice);
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DATABASE_INDEX);
            jedis.hset(SOC_LOW_REDIS_KEY, vehicleId, json);
        });

        result[0] = json;

        LOG.debug("VID:{} SOC过低开始通知发送 MSGID:{}", vehicleId, socLowStartNotice.get("msgId"));
    }

    private void processChargeCars(
        final @NotNull ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final TeConsumer<@NotNull String, @NotNull Double, @NotNull Double> processChargeCars) {

        final String longitudeString = data.get(DataKey._2502_LONGITUDE);
        final String latitudeString = data.get(DataKey._2503_LATITUDE);
        try {
            final double longitude = NumberUtils.toDouble(longitudeString, 0);
            final double latitude = NumberUtils.toDouble(latitudeString, 0);
            //检查经纬度是否为无效值
            final double absLongitude = Math.abs(longitude);
            final double absLatitude = Math.abs(latitude);
            if (0 == absLongitude || absLongitude > DataKey.MAX_2502_LONGITUDE || 0 == absLatitude || absLatitude > DataKey.MAX_2503_LATITUDE) {
                return;
            }
            // 附近补电车信息
            processChargeCars.accept(vehicleId, longitude / 1000000d, latitude / 1000000d);
        } catch (final Exception e) {
            LOG.warn("获取补电车信息的时出现异常", e);
        }
    }

    // endregion SOC过低开始

    // region SOC过低结束

    private void socLowEndReset(
        final @NotNull ImmutableMap<String, String> data,
        @NotNull final String vehicleId,
        @NotNull final String platformReceiverTimeString,
        final int soc) {

        final String longitudeString = data.get(DataKey._2502_LONGITUDE);
        final String latitudeString = data.get(DataKey._2503_LATITUDE);
        @NotNull final String location = StringUtils.defaultString(
            DataUtils.buildLocation(
                longitudeString,
                latitudeString
            )
        );

        vehicleNoticeCache.put(
            vehicleId,
            new ImmutableMap.Builder<String, String>()
                .put("etime", platformReceiverTimeString)
                .put("elocation", location)
                .put("ethreshold", String.valueOf(ConfigUtils.getSysDefine().getNoticeSocLowEndTriggerThreshold()))
                .put("esoc", String.valueOf(soc))
                .build()
        );

        LOG.trace("VID:{} SOC过低结束首帧初始化", vehicleId);
    }

    private void socLowEndOverflow(
        final int count,
        final long timeout,
        @NotNull final String vehicleId,
        @NotNull final String[] result) {

        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DATABASE_INDEX);

            final ImmutableMap<String, String> socLowBeginNotice = loadSocLowNoticeFromRedis(jedis, vehicleId);
            if(MapUtils.isNotEmpty(socLowBeginNotice)) {
                final long currentTimeMillis = System.currentTimeMillis();
                final String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

                final Map<String, String> socLowEndNotice = Maps.newHashMap(
                    socLowBeginNotice
                );
                socLowEndNotice.putAll(vehicleNoticeCache.get(vehicleId));
                socLowEndNotice.put("status", NOTICE_STATUS_END);
                socLowEndNotice.put("econtinue", String.valueOf(count));
                socLowEndNotice.put("elazy", String.valueOf(timeout));
                socLowEndNotice.put("noticeTime", noticeTime);

                final String json = JSON_UTILS.toJson(socLowEndNotice);
                jedis.hdel(SOC_LOW_REDIS_KEY, vehicleId);

                result[0] = json;

                LOG.debug("VID:{} SOC过低结束通知发送 MSGID:{}", vehicleId, socLowEndNotice.get("msgId"));
            }
        });
    }

    // endregion SOC过低结束
}
