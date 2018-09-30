package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.ObjectExtension;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.tool.DelaySwitch;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

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

    @NotNull
    private ImmutableMap<String, String> startNotice = ImmutableMap.of();

    @NotNull
    private ImmutableMap<String, String> endNotice = ImmutableMap.of();

    public AlarmStatus(final String vehicleId) {
        this.vehicleId = vehicleId;
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
        startNotice = new ImmutableMap.Builder<String, String>()
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

        @NotNull final String platformReceiveTimeString = data.get(
            DataKey._9999_PLATFORM_RECEIVE_TIME);
        final String alarmId = buildAlarmId(vehicleId, platformReceiveTimeString, ruleId);
        final int alarmLevel = parseAlarmLevel(level, data);
        final String ruleName = ObjectExtension.defaultIfNull(rule.ruleName, "");
        final String left2DataKey = ObjectExtension.defaultIfNull(rule.left2DataKey, "");
        final String leftExpression = ObjectExtension.defaultIfNull(rule.leftExpression, "");
        final String right2Value = ObjectExtension.defaultIfNull(rule.right2Value, "");

        final ImmutableMap<String, String> startNotice = new ImmutableMap.Builder<String, String>()
            .putAll(this.startNotice)
            .put(DataKey.VEHICLE_ID, vehicleId)
            .put("ALARM_ID", alarmId)
            .put("STATUS", "1")
            .put("TIME", platformReceiveTimeString)
            .put("CONST_ID", ruleId)
            .put("ALARM_LEVEL", String.valueOf(alarmLevel))
            //
            .put("ALARM_NAME", ruleName)
            .put("LEFT1", rule.left1DataKey)
            .put("left1_use_prev", String.valueOf(rule.left1UsePrev))
            .put("LEFT2", left2DataKey)
            .put("left2_use_prev", String.valueOf(rule.left2UsePrev))
            .put("arithmetic_expression", leftExpression)
            .put("RIGHT1", rule.right1Value)
            .put("RIGHT2", right2Value)
            .put("logic_expression", rule.middleExpression)
            .put("sNoticeTime", DataUtils.buildFormatTime(System.currentTimeMillis()))
            .put("sThreshold", String.valueOf(positiveThreshold))
            .put("sTimeout", String.valueOf(positiveTimeout))
            .build();

        this.startNotice = startNotice;
        noticeCallback.accept(startNotice);
    }

    private void endReset() {
        endNotice = new ImmutableMap.Builder<String, String>()
            .putAll(startNotice)
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

        @NotNull final String platformReceiveTimeString = data.get(
            DataKey._9999_PLATFORM_RECEIVE_TIME);
        final String alarmId = buildAlarmId(vehicleId, platformReceiveTimeString, ruleId);
        final int alarmLevel = parseAlarmLevel(level, data);
        final String ruleName = ObjectExtension.defaultIfNull(rule.ruleName, "");
        final String left2DataKey = ObjectExtension.defaultIfNull(rule.left2DataKey, "");
        final String leftExpression = ObjectExtension.defaultIfNull(rule.leftExpression, "");
        final String right2Value = ObjectExtension.defaultIfNull(rule.right2Value, "");

        final ImmutableMap<String, String> endNotice = new ImmutableMap.Builder<String, String>()
            .putAll(this.endNotice)
            .put(DataKey.VEHICLE_ID, vehicleId)
            .put("ALARM_ID", alarmId)
            .put("STATUS", "3")
            .put("TIME", platformReceiveTimeString)
            .put("CONST_ID", ruleId)
            .put("ALARM_LEVEL", String.valueOf(alarmLevel))
            //
            .put("ALARM_NAME", ruleName)
            .put("LEFT1", rule.left1DataKey)
            .put("left1_use_prev", String.valueOf(rule.left1UsePrev))
            .put("LEFT2", left2DataKey)
            .put("left2_use_prev", String.valueOf(rule.left2UsePrev))
            .put("arithmetic_expression", leftExpression)
            .put("RIGHT1", rule.right1Value)
            .put("RIGHT2", right2Value)
            .put("logic_expression", rule.middleExpression)
            .put("eNoticeTime", DataUtils.buildFormatTime(System.currentTimeMillis()))
            .put("eThreshold", String.valueOf(negativeThreshold))
            .put("eTimeout", String.valueOf(negativeTimeout))
            .build();

        this.endNotice = endNotice;
        noticeCallback.accept(endNotice);
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
}
