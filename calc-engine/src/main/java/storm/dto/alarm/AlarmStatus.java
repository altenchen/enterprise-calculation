package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.LocaleUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.ObjectExtension;
import storm.system.DataKey;
import storm.tool.MultiDelaySwitch;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.Map;
import java.util.function.Consumer;

/**
 * @author: xzp
 * @date: 2018-09-30
 * @description: 平台报警状态
 */
public final class AlarmStatus {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(AlarmStatus.class);

    public static final String NOTICE_STATUS_KEY = "STATUS";

    public static final String NOTICE_STATUS_START = "1";

    public static final String NOTICE_STATUS_END = "3";

    private final MultiDelaySwitch<Boolean> delaySwitch = new MultiDelaySwitch<Boolean>()
        .setThresholdTimes(Boolean.TRUE, ConfigUtils.getSysDefine().getAlarmStartTriggerContinueCount())
        .setTimeoutMillisecond(Boolean.TRUE, ConfigUtils.getSysDefine().getAlarmStartTriggerTimeoutMillisecond())
        .setThresholdTimes(Boolean.FALSE, ConfigUtils.getSysDefine().getAlarmStopTriggerContinueCount())
        .setTimeoutMillisecond(Boolean.FALSE, ConfigUtils.getSysDefine().getAlarmStopTriggerTimeoutMillisecond());

    private final String vehicleId;

    /**
     * 持续状态
     */
    @NotNull
    private ImmutableMap<String, String> continueStatus = ImmutableMap.of();

    public AlarmStatus(
        @NotNull final String vehicleId,
        final boolean started) {

        this.vehicleId = vehicleId;
        setStatus(started);
    }

    public void updateVehicleAlarmData(
        final boolean result,
        final long platformReceiveTime,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final EarlyWarn rule,
        @NotNull final Consumer<ImmutableMap<String, String>> noticeCallback) {
        if(result) {
            //上次+本次 或 本次+上次 立即触发
            if(rule.left1UsePrev != rule.left2UsePrev) {
                if (BooleanUtils.isNotTrue(getStatus())) {
                    this.startReset(data);
                    startOverflow(
                        1,
                        0,
                        ruleId,
                        level,
                        data,
                        cache,
                        rule,
                        noticeCallback);
                    setStatus(true);
                }
            }

            delaySwitch.increase(
                true,
                platformReceiveTime,
                status -> startReset(data),
                (status, positiveThreshold, positiveTimeout)-> {
                    startOverflow(
                        positiveThreshold,
                        positiveTimeout,
                        ruleId,
                        level,
                        data,
                        cache,
                        rule,
                        noticeCallback);
                });
        } else {
            delaySwitch.increase(
                false,
                platformReceiveTime,
                status->endReset(data),
                (status, negativeThreshold, negativeTimeout)-> {
                    endOverflow(
                        negativeThreshold,
                        negativeTimeout,
                        ruleId,
                        level,
                        data,
                        rule,
                        noticeCallback
                    );
                });
        }
    }

    private void startReset(@NotNull final ImmutableMap<String, String> data) {
        @NotNull final String platformReceiveTimeString = data.get(
                DataKey._9999_PLATFORM_RECEIVE_TIME);
        continueStatus = new ImmutableMap.Builder<String, String>()
            .put("TIME", platformReceiveTimeString)
            .build();
    }

    private void startOverflow(
        final int positiveThreshold,
        final long positiveTimeout,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final EarlyWarn rule,
        @NotNull final Consumer<ImmutableMap<String, String>> noticeCallback) {

        final Boolean status = getStatus();
        if (BooleanUtils.isNotTrue(status)) {
            final ImmutableMap<String, String> startNotice = buildStartNotice(
                positiveThreshold,
                positiveTimeout,
                ruleId,
                level,
                data,
                cache,
                rule);

            noticeCallback.accept(startNotice);
        }
    }
    
    @NotNull
    private ImmutableMap<String, String> buildStartNotice(
        final int positiveThreshold,
        final long positiveTimeout,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final EarlyWarn rule) {
        final String alarmId = buildAlarmId(vehicleId, ruleId);
        final int alarmLevel = parseAlarmLevel(level, data);
        final String ruleName = ObjectExtension.defaultIfNull(rule.ruleName, "");
        final String left2DataKey = ObjectExtension.defaultIfNull(rule.left2DataKey, "");
        final String leftExpression = ObjectExtension.defaultIfNull(rule.leftExpression, "");
        final String right2Value = ObjectExtension.defaultIfNull(rule.right2Value, "");
        //增加经纬度信息
        final String location = getLocation(data, cache);

        final Map<String, String> startNotice = Maps.newHashMap();
        startNotice.putAll(continueStatus);
        startNotice.put(DataKey.VEHICLE_ID, vehicleId);
        startNotice.put("ALARM_ID", alarmId);
        startNotice.put(NOTICE_STATUS_KEY, NOTICE_STATUS_START);
        startNotice.put("CONST_ID", ruleId);
        startNotice.put("ALARM_LEVEL", String.valueOf(alarmLevel));
        //
        startNotice.put("ALARM_NAME", ruleName);
        startNotice.put("LEFT1", rule.left1DataKey);
        startNotice.put("left1_use_prev", String.valueOf(rule.left1UsePrev));
        startNotice.put("LEFT2", left2DataKey);
        startNotice.put("left2_use_prev", String.valueOf(rule.left2UsePrev));
        startNotice.put("arithmetic_expression", leftExpression);
        startNotice.put("RIGHT1", rule.right1Value);
        startNotice.put("RIGHT2", right2Value);
        startNotice.put("logic_expression", rule.middleExpression);
        startNotice.put("sNoticeTime", DataUtils.buildFormatTime(System.currentTimeMillis()));
        startNotice.put("sThreshold", String.valueOf(positiveThreshold));
        startNotice.put("sTimeout", String.valueOf(positiveTimeout));
        //增加位置信息
        startNotice.put("sLocation", location);


        return ImmutableMap.copyOf(startNotice);
    }

    private void endReset(@NotNull final ImmutableMap<String, String> data) {
        @NotNull final String platformReceiveTimeString = data.get(
                DataKey._9999_PLATFORM_RECEIVE_TIME);
        continueStatus = new ImmutableMap.Builder<String, String>()
            .put("TIME", platformReceiveTimeString)
            .build();
    }

    private void endOverflow(
        final int negativeThreshold,
        final long negativeTimeout,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final EarlyWarn rule,
        @NotNull final Consumer<ImmutableMap<String, String>> noticeCallback) {

        final Boolean status = getStatus();
        if(BooleanUtils.isNotFalse(status)) {

            final ImmutableMap<String, String> endNotice = buildEndNotice(
                negativeThreshold,
                negativeTimeout,
                ruleId,
                level,
                data,
                rule);

            noticeCallback.accept(endNotice);
        }
    }

    @NotNull
    private ImmutableMap<String, String> buildEndNotice(
        final int negativeThreshold,
        final long negativeTimeout,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final EarlyWarn rule){
        final String alarmId = buildAlarmId(vehicleId, ruleId);
        final int alarmLevel = parseAlarmLevel(level, data);
        final String ruleName = ObjectExtension.defaultIfNull(rule.ruleName, "");
        final String left2DataKey = ObjectExtension.defaultIfNull(rule.left2DataKey, "");
        final String leftExpression = ObjectExtension.defaultIfNull(rule.leftExpression, "");
        final String right2Value = ObjectExtension.defaultIfNull(rule.right2Value, "");

        final Map<String, String> endNotice = Maps.newHashMap();
        endNotice.putAll(continueStatus);
        endNotice.put(DataKey.VEHICLE_ID, vehicleId);
        endNotice.put("ALARM_ID", alarmId);
        endNotice.put(NOTICE_STATUS_KEY, NOTICE_STATUS_END);
        endNotice.put("CONST_ID", ruleId);
        endNotice.put("ALARM_LEVEL", String.valueOf(alarmLevel));
        //
        endNotice.put("ALARM_NAME", ruleName);
        endNotice.put("LEFT1", rule.left1DataKey);
        endNotice.put("left1_use_prev", String.valueOf(rule.left1UsePrev));
        endNotice.put("LEFT2", left2DataKey);
        endNotice.put("left2_use_prev", String.valueOf(rule.left2UsePrev));
        endNotice.put("arithmetic_expression", leftExpression);
        endNotice.put("RIGHT1", rule.right1Value);
        endNotice.put("RIGHT2", right2Value);
        endNotice.put("logic_expression", rule.middleExpression);
        endNotice.put("eNoticeTime", DataUtils.buildFormatTime(System.currentTimeMillis()));
        endNotice.put("eThreshold", String.valueOf(-negativeThreshold));
        endNotice.put("eTimeout", String.valueOf(negativeTimeout));

        return ImmutableMap.copyOf(endNotice);
    }

    private int parseAlarmLevel(
        final int level,
        @NotNull final ImmutableMap<String, String> data
    ) {
        return 0 == level ? NumberUtils.toInt(
            data.get(DataKey._2920_ALARM_STATUS),
            level) : level;
    }

    /**
     * 获经纬度信息
     * @param data 从当前数据获取
     * @param cache 如果当前数据不存在，则从缓存的上一帧有效数据中获取
     * @return 经纬度信息
     */
    private String getLocation(@NotNull final ImmutableMap<String, String> data,
                               @NotNull final ImmutableMap<String, String> cache) {
        if (data.containsKey(DataKey._2502_LONGITUDE) && data.containsKey(DataKey._2503_LATITUDE)) {
            return DataUtils.buildLocation(data.get(DataKey._2502_LONGITUDE), data.get(DataKey._2503_LATITUDE));
        } else if (cache.containsKey(DataKey._2502_LONGITUDE) && cache.containsKey(DataKey._2503_LATITUDE)){
            return DataUtils.buildLocation(cache.get(DataKey._2502_LONGITUDE), cache.get(DataKey._2503_LATITUDE));
        } else {
            return null;
        }
    }

    @NotNull
    private String buildAlarmId(
        @NotNull final String vehicleId,
        @NotNull final String ruleId) {
        //noinspection StringBufferReplaceableByString
        return new StringBuilder()
            .append(vehicleId)
            .append("_")
            .append(ruleId)
            .toString();
    }

    /**
     * 设置报警状态
     * true 开始
     * false 结束
     * @param status 报警状态
     */
    private void setStatus(final boolean status) {
        delaySwitch.setSwitchStatus(status);
    }

    /**
     * 获取报警状态
     * null 未知
     * true 开始
     * false 结束
     * @return 报警状态
     */
    @Nullable
    @Contract(pure = true)
    public Boolean getStatus() {
        return delaySwitch.getSwitchStatus();
    }
}
