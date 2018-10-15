package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.ObjectExtension;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.tool.DelaySwitch;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.Map;
import java.util.Properties;
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

    /**
     * 触发平台报警开始需要的连续次数
     */
    private static final int ALARM_START_TRIGGER_CONTINUE_COUNT;

    /**
     * 触发平台报警开始需要的持续时长
     */
    private static final int ALARM_START_TRIGGER_TIMEOUT_MILLISECOND;

    /**
     * 触发平台报警结束需要的连续次数
     */
    private static final int ALARM_STOP_TRIGGER_CONTINUE_COUNT;

    /**
     * 触发平台报警结束需要的持续时长
     */
    private static final int ALARM_STOP_TRIGGER_TIMEOUT_MILLISECOND;

    static {

        final ConfigUtils configUtils = ConfigUtils.getInstance();
        final Properties sysDefine = configUtils.sysDefine;

        ALARM_START_TRIGGER_CONTINUE_COUNT = NumberUtils.toInt(
            sysDefine.getProperty(SysDefine.ALARM_START_TRIGGER_CONTINUE_COUNT),
            3);

        ALARM_START_TRIGGER_TIMEOUT_MILLISECOND = NumberUtils.toInt(
            sysDefine.getProperty(SysDefine.ALARM_START_TRIGGER_TIMEOUT_MILLISECOND),
            30000);

        ALARM_STOP_TRIGGER_CONTINUE_COUNT = NumberUtils.toInt(
            sysDefine.getProperty(SysDefine.ALARM_START_TRIGGER_CONTINUE_COUNT),
            3);

        ALARM_STOP_TRIGGER_TIMEOUT_MILLISECOND = NumberUtils.toInt(
            sysDefine.getProperty(SysDefine.ALARM_START_TRIGGER_TIMEOUT_MILLISECOND),
            30000);
    }

    private final DelaySwitch delaySwitch = new DelaySwitch(
        ALARM_START_TRIGGER_CONTINUE_COUNT,
        ALARM_START_TRIGGER_TIMEOUT_MILLISECOND,
        ALARM_STOP_TRIGGER_CONTINUE_COUNT,
        ALARM_STOP_TRIGGER_TIMEOUT_MILLISECOND
    );

    private final String vehicleId;

    /**
     * 持续状态
     */
    @NotNull
    private ImmutableMap<String, String> continueStatus = ImmutableMap.of();

    /**
     * 开始通知
     */
    @NotNull
    private ImmutableMap<String, String> startNotice;

    public AlarmStatus(@NotNull final String vehicleId) {
        this(vehicleId, ImmutableMap.of());
    }

    public AlarmStatus(
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> startNotice) {

        this.vehicleId = vehicleId;
        this.startNotice = startNotice;
        delaySwitch.setSwitchStatus(1);
    }

    public void updateVehicleAlarmData(
        final boolean result,
        final long platformReceiveTime,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final EarlyWarn rule,
        @NotNull final Consumer<ImmutableMap<String, String>> noticeCallback) {
        if(result) {
            delaySwitch.positiveIncrease(
                platformReceiveTime,
                this::startReset,
                (positiveThreshold, positiveTimeout)-> startOverflow(
                    positiveThreshold,
                    positiveTimeout,
                    ruleId,
                    level,
                    data,
                    rule,
                    noticeCallback));
        } else {
            delaySwitch.negativeIncrease(
                platformReceiveTime,
                this::endReset,
                (negativeThreshold, negativeTimeout)-> endOverflow(
                    negativeThreshold,
                    negativeTimeout,
                    ruleId,
                    level,
                    data,
                    rule,
                    noticeCallback
                ));
        }
    }

    private void startReset() {
        continueStatus = new ImmutableMap.Builder<String, String>()
            .build();
    }

    private void startOverflow(
        final int positiveThreshold,
        final long positiveTimeout,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final EarlyWarn rule,
        @NotNull final Consumer<ImmutableMap<String, String>> noticeCallback) {

        final ImmutableMap<String, String> startNotice = buildStartNotice(
            positiveThreshold,
            positiveTimeout,
            ruleId,
            level,
            data,
            rule);

        this.startNotice = startNotice;
        noticeCallback.accept(startNotice);
    }
    
    @NotNull
    private ImmutableMap<String, String> buildStartNotice(
        final int positiveThreshold,
        final long positiveTimeout,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final EarlyWarn rule) {
        
        @NotNull final String platformReceiveTimeString = data.get(
            DataKey._9999_PLATFORM_RECEIVE_TIME);
        final String alarmId = buildAlarmId(vehicleId, platformReceiveTimeString, ruleId);
        final int alarmLevel = parseAlarmLevel(level, data);
        final String ruleName = ObjectExtension.defaultIfNull(rule.ruleName, "");
        final String left2DataKey = ObjectExtension.defaultIfNull(rule.left2DataKey, "");
        final String leftExpression = ObjectExtension.defaultIfNull(rule.leftExpression, "");
        final String right2Value = ObjectExtension.defaultIfNull(rule.right2Value, "");

        final Map<String, String> startNotice = Maps.newHashMap();
        startNotice.putAll(this.continueStatus);
        startNotice.put(DataKey.VEHICLE_ID, vehicleId);
        startNotice.put("ALARM_ID", alarmId);
        startNotice.put(NOTICE_STATUS_KEY, NOTICE_STATUS_START);
        startNotice.put("TIME", platformReceiveTimeString);
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

        return ImmutableMap.copyOf(startNotice);
    }

    private void endReset() {
        continueStatus = new ImmutableMap.Builder<String, String>()
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

        final ImmutableMap<String, String> endNotice = buildEndNotice(
            negativeThreshold,
            negativeTimeout,
            ruleId,
            level,
            data,
            rule);

        this.startNotice = ImmutableMap.of();
        noticeCallback.accept(endNotice);
    }

    @NotNull
    private ImmutableMap<String, String> buildEndNotice(
        final int negativeThreshold,
        final long negativeTimeout,
        @NotNull final String ruleId,
        final int level,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final EarlyWarn rule){

        @NotNull final String platformReceiveTimeString = data.get(
            DataKey._9999_PLATFORM_RECEIVE_TIME);
        final String alarmId = buildAlarmId(vehicleId, platformReceiveTimeString, ruleId);
        final int alarmLevel = parseAlarmLevel(level, data);
        final String ruleName = ObjectExtension.defaultIfNull(rule.ruleName, "");
        final String left2DataKey = ObjectExtension.defaultIfNull(rule.left2DataKey, "");
        final String leftExpression = ObjectExtension.defaultIfNull(rule.leftExpression, "");
        final String right2Value = ObjectExtension.defaultIfNull(rule.right2Value, "");

        final Map<String, String> endNotice = Maps.newHashMap();
        endNotice.putAll(this.continueStatus);
        endNotice.put(DataKey.VEHICLE_ID, vehicleId);
        endNotice.put("ALARM_ID", alarmId);
        endNotice.put(NOTICE_STATUS_KEY, NOTICE_STATUS_END);
        endNotice.put("TIME", platformReceiveTimeString);
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
        endNotice.put("eThreshold", String.valueOf(negativeThreshold));
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

    @NotNull
    private String buildAlarmId(
        @NotNull final String vehicleId,
        @NotNull final String platformReceiveTimeString,
        @NotNull final String ruleId) {
        return new StringBuilder()
            .append(vehicleId)
            .append("_")
            .append(platformReceiveTimeString)
            .append("_")
            .append(ruleId)
            .toString();
    }

    @Nullable
    @Contract(pure = true)
    private Boolean getStatus() {
        final int switchStatus = delaySwitch.getSwitchStatus();
        if(switchStatus > 0) {
            return true;
        } else if(switchStatus < 0) {
            return false;
        } else {
            return null;
        }
    }

    public void finishNoticeIfStarted(
        @NotNull final Consumer<ImmutableMap<String, String>> noticeCallback) {
        if (BooleanUtils.isTrue(getStatus())) {
            if(MapUtils.isNotEmpty(startNotice)) {
                final Map<String, String> endNotice = Maps.newHashMap();
                endNotice.putAll(this.startNotice);
                endNotice.put("STATUS", NOTICE_STATUS_END);
                endNotice.put("eNoticeTime", DataUtils.buildFormatTime(System.currentTimeMillis()));
                endNotice.put("reason", "rule_unable");

                noticeCallback.accept(ImmutableMap.copyOf(endNotice));
            }
        }
    }
}
