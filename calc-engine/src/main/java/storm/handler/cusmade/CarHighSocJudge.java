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

import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
/**
 * SOC过高预警
 * 修改[ xzj ]：
 *     重构相关逻辑
 *
 * @author 于心沼, xzj
 *
 */
public class CarHighSocJudge  {

    private static final Logger LOG = LoggerFactory.getLogger(CarHighSocJudge.class);

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();
    private static final String NOTICE_STATUS_KEY = "status";
    private static final String NOTICE_STATUS_START = "1";
    private static final String NOTICE_STATUS_END = "3";

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final int REDIS_DATABASE_INDEX = 6;
    private static final String SOC_HIGH_REDIS_KEY = "vehCache.qy.soc.high.notice";
    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

    @NotNull
    @Contract(" -> new")
    private static DelaySwitch buildDelaySwitch() {
        return new DelaySwitch(
                ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerContinueCount(),
                ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerTimeoutMillisecond(),
                ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerContinueCount(),
                ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerTimeoutMillisecond());
    }

    // region 状态表

    /**
     * 车辆状态缓存表 <vehicleId, DelaySwitch></>
     */
    private final Map<String, DelaySwitch> vehicleStatus = Maps.newHashMap();

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void syncDelaySwitchConfig() {
        vehicleStatus.forEach((vehicleId, delaySwitch)->{
            delaySwitch.setPositiveThreshold(ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerContinueCount());
            delaySwitch.setPositiveTimeout(ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerTimeoutMillisecond());
            delaySwitch.setNegativeThreshold(ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerContinueCount());
            delaySwitch.setNegativeTimeout(ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerTimeoutMillisecond());
        });
    }

    @NotNull
    private DelaySwitch ensureVehicleStatus(@NotNull final String vehicleId) {
        return vehicleStatus.computeIfAbsent(
                vehicleId,
                k -> JEDIS_POOL_UTILS.useResource(jedis -> {
                    jedis.select(REDIS_DATABASE_INDEX);

                    final String json = jedis.hget(SOC_HIGH_REDIS_KEY, vehicleId);
                    if (StringUtils.isNotBlank(json)) {
                        final ImmutableMap<String, String> startNotice = loadSocHighNoticeFromRedis(jedis, vehicleId);
                        final String status = startNotice.get(NOTICE_STATUS_KEY);
                        if(NOTICE_STATUS_START.equals(status)) {
                            return buildDelaySwitch().setSwitchStatus(true);
                        } else if(NOTICE_STATUS_END.equals(status)) {
                            LOG.warn("VID:{} REDIS DB:{} KEY:{} 中已结束的平台报警通知 {}", vehicleId, REDIS_DATABASE_INDEX, SOC_HIGH_REDIS_KEY, json);
                            jedis.hdel(SOC_HIGH_REDIS_KEY, vehicleId);
                        } else {
                            LOG.warn("VID:{} REDIS DB:{} KEY:{} 中状态为 {} 的异常平台报警通知 {}", vehicleId, REDIS_DATABASE_INDEX, SOC_HIGH_REDIS_KEY, status, json);
                            jedis.hdel(SOC_HIGH_REDIS_KEY, vehicleId);
                        }
                    }
                    return buildDelaySwitch().setSwitchStatus(false);
                })
        );
    }

    @NotNull
    private ImmutableMap<String, String> loadSocHighNoticeFromRedis(
            @NotNull final Jedis jedis,
            @NotNull final String vehicleId) {

        final String json = jedis.hget(SOC_HIGH_REDIS_KEY, vehicleId);
        if (StringUtils.isNotBlank(json)) {
            return ImmutableMap.copyOf(
                    ObjectExtension.defaultIfNull(
                            JSON_UTILS.fromJson(
                                    json,
                                    TREE_MAP_STRING_STRING_TYPE,
                                    e -> {
                                        LOG.warn("VID:{} REDIS DB:{} KEY:{} 中不是合法json的SOC过高通知 {}", vehicleId, REDIS_DATABASE_INDEX, SOC_HIGH_REDIS_KEY, json);
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
     * @return 如果产生高电量通知, 则填充通知, 否则为空集合.
     */
    @SuppressWarnings("WeakerAccess")
    public String processFrame(
            @NotNull final ImmutableMap<String, String> data) {

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

        // 检验SOC是否大于等于过高开始阈值
        if(soc >= ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerThreshold()) {
            final String[] result = new String[1];
            final DelaySwitch delaySwitch = ensureVehicleStatus(vehicleId);
            delaySwitch.positiveIncrease(
                    platformReceiverTime,
                    () -> socHighBeginReset(data, vehicleId, platformReceiverTimeString, soc),
                    (count, timeout) -> {
                        socHighBeginOverflow(count, timeout, vehicleId, result);
                    }
            );
            return result[0];
        }
        // 检验SOC是否小于过高结束阈值
        else if(soc < ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerThreshold()) {
            final String[] result = new String[1];
            final DelaySwitch delaySwitch = ensureVehicleStatus(vehicleId);
            delaySwitch.negativeIncrease(
                    platformReceiverTime,
                    () -> socHighEndReset(data, vehicleId, platformReceiverTimeString, soc),
                    (count, timeout) -> socHighEndOverflow(count, timeout, vehicleId, result)
            );
            return result[0];
        } else {
            return null;
        }
    }

    // region SOC过高开始

    private void socHighBeginReset(
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
                        .put("msgType", NoticeType.SOC_HIGH_NOTICE)
                        .put("msgId", UUID.randomUUID().toString())
                        .put("vid", vehicleId)
                        .put("stime", platformReceiverTimeString)
                        .put("location", location)
                        .put("slocation", location)
                        .put("sthreshold", String.valueOf(ConfigUtils.getSysDefine().getNoticeSocHighBeginTriggerThreshold()))
                        .put("ssoc", String.valueOf(soc))
                        .build()
        );

        LOG.trace("VID:{} SOC过高开始首帧缓存初始化", vehicleId);
    }

    private void socHighBeginOverflow(
            final int count,
            final long timeout,
            @NotNull final String vehicleId,
            @NotNull final String[] result){

        final long currentTimeMillis = System.currentTimeMillis();
        final String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

        final Map<String, String> socHighStartNotice = Maps.newHashMap(
                vehicleNoticeCache.get(vehicleId)
        );
        socHighStartNotice.put("status", NOTICE_STATUS_START);
        socHighStartNotice.put("scontinue", String.valueOf(count));
        socHighStartNotice.put("slazy", String.valueOf(timeout));
        socHighStartNotice.put("noticeTime", noticeTime);

        final String json = JSON_UTILS.toJson(socHighStartNotice);
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DATABASE_INDEX);
            jedis.hset(SOC_HIGH_REDIS_KEY, vehicleId, json);
        });

        result[0] = json;

        LOG.debug("VID:{} SOC过高开始通知发送 MSGID:{}", vehicleId, socHighStartNotice.get("msgId"));
    }
    // endregion SOC过高开始

    // region SOC过高结束

    private void socHighEndReset(
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
                        .put("ethreshold", String.valueOf(ConfigUtils.getSysDefine().getNoticeSocHighEndTriggerThreshold()))
                        .put("esoc", String.valueOf(soc))
                        .build()
        );

        LOG.trace("VID:{} SOC过高结束首帧初始化", vehicleId);
    }

    private void socHighEndOverflow(
            final int count,
            final long timeout,
            @NotNull final String vehicleId,
            @NotNull final String[] result) {

        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DATABASE_INDEX);

            final ImmutableMap<String, String> socHighBeginNotice = loadSocHighNoticeFromRedis(jedis, vehicleId);
            if(MapUtils.isNotEmpty(socHighBeginNotice)) {
                final long currentTimeMillis = System.currentTimeMillis();
                final String noticeTime = DateFormatUtils.format(currentTimeMillis, FormatConstant.DATE_FORMAT);

                final Map<String, String> socHighEndNotice = Maps.newHashMap(
                        socHighBeginNotice
                );
                socHighEndNotice.putAll(vehicleNoticeCache.get(vehicleId));
                socHighEndNotice.put("status", NOTICE_STATUS_END);
                socHighEndNotice.put("econtinue", String.valueOf(count));
                socHighEndNotice.put("elazy", String.valueOf(timeout));
                socHighEndNotice.put("noticeTime", noticeTime);

                final String json = JSON_UTILS.toJson(socHighEndNotice);
                jedis.hdel(SOC_HIGH_REDIS_KEY, vehicleId);

                result[0] = json;

                LOG.debug("VID:{} SOC过高结束通知发送 MSGID:{}", vehicleId, socHighEndNotice.get("msgId"));
            }
        });
    }

    // endregion SOC过高结束
}
