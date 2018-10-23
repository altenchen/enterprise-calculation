package storm.util;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.system.SysDefine;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author wza
 * 从Redis读取配置
 */
public final class ParamsRedisUtil {

    // region 静态区

    /**
     * 日志组件
     */
    private static final Logger LOG = LoggerFactory.getLogger(ParamsRedisUtil.class);

    /**
     * SOC百分值
     */
    public static final String LT_ALARM_SOC_PERCENT = "lt.alarm.soc";
    /**
     * 车辆达到闲置状态的时间阈值, 默认1天, 单位毫秒
     */
    public static final String VEHICLE_IDLE_TIMEOUT_MILLISECOND = "vehicle.idle.timeout.millisecond";
    /**
     * 连续多少帧没有gps，算为未定位车辆
     */
    @SuppressWarnings("SpellCheckingInspection")
    public static final String GPS_NOVALUE_CONTINUE_NO = "gps.novalue.continue.no";
    /**
     * 连续多少帧有gps，算为定位车辆
     */
    @SuppressWarnings("SpellCheckingInspection")
    public static final String GPS_HASVALUE_CONTINUE_NO = "gps.hasvalue.continue.no";
    /**
     * 连续多少帧没有can状态，算为无can状态车辆
     */
    @SuppressWarnings("SpellCheckingInspection")
    public static final String CAN_NOVALUE_CONTINUE_NO = "can.novalue.continue.no";
    /**
     * 连续多少帧有can状态，算为有can状态车辆
     */
    @SuppressWarnings("SpellCheckingInspection")
    public static final String CAN_HASVALUE_CONTINUE_NO = "can.hasvalue.continue.no";
    /**
     * 里程跳变数
     */
    public static final String MILE_HOP_NUM = "mile.hop.num";
    /**
     * 秒
     */
    public static final String CAN_JUDGE_SECOND = "can.judge.time";
    /**
     * 配置来源于Redis哪个库
     */
    public static final int CONFIG_DATABASE_INDEX = 4;
    /**
     * 配置来自库中哪个键的Hash
     */
    public static final String CAL_QY_CONF = "cal.qy.conf";

    /**
     * 追踪的车辆ID
     */
    private static final String TRACE_VEHICLE_ID = "trace.vehicle.id";

    static {

    }

    /**
     * 单例对象
     */
    private static final ParamsRedisUtil ourInstance = new ParamsRedisUtil();

    @Contract(pure = true)
    public static ParamsRedisUtil getInstance() {
        return ourInstance;
    }
    // endregion 静态区

    /**
     * 参数缓存
     */
    public final Map<String, Object> PARAMS = new TreeMap<>();

    private final DataToRedis redis = new DataToRedis();

    // 实例初始化块
    {
        resetToDefault();
        initParams();
    }

    private ParamsRedisUtil() {
    }

    /**
     * 设置默认值
     */
    private void resetToDefault() {
        PARAMS.put(LT_ALARM_SOC_PERCENT, 10);
        PARAMS.put(VEHICLE_IDLE_TIMEOUT_MILLISECOND, TimeUnit.DAYS.toMillis(1));
        PARAMS.put(GPS_NOVALUE_CONTINUE_NO, 5);
        PARAMS.put(GPS_HASVALUE_CONTINUE_NO, 10);
        PARAMS.put(CAN_NOVALUE_CONTINUE_NO, 5);
        PARAMS.put(CAN_HASVALUE_CONTINUE_NO, 10);
        PARAMS.put(MILE_HOP_NUM, 2);
        PARAMS.put(SysDefine.GPS_JUDGE_TIME, 60);
        PARAMS.put(CAN_JUDGE_SECOND, 60);

        PARAMS.put(SysDefine.RULE_OVERRIDE, "default");
        PARAMS.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT, 3);
        PARAMS.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND, 0);
        PARAMS.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT, 3);
        PARAMS.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND, 0);
        PARAMS.put(TRACE_VEHICLE_ID, "test\\d+");

        // 规则覆盖
        final String ruleOverride = ConfigUtils.getSysDefine().getRuleOverride();
        if (!StringUtils.isBlank(ruleOverride)) {
            PARAMS.put(SysDefine.RULE_OVERRIDE, ruleOverride);
        }

        // 触发CAN故障需要的连续帧数
        PARAMS.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT, ConfigUtils.getSysDefine().getNoticeCanFaultTriggerContinueCount());

        // 触发CAN故障需要的持续时长
        PARAMS.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND, ConfigUtils.getSysDefine().getNoticeCanFaultTriggerTimeoutMillisecond());

        // 触发CAN正常需要的连续帧数
        PARAMS.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT, ConfigUtils.getSysDefine().getNoticeCanNormalTriggerContinueCount());

        // 触发CAN正常需要的持续时长
        PARAMS.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND, ConfigUtils.getSysDefine().getNoticeCanNormalTriggerTimeoutMillisecond());
    }

    private void initParams() {
        Map<String, String> paramCache = redis.getMap(CONFIG_DATABASE_INDEX, CAL_QY_CONF);
        if (null != paramCache && paramCache.size() > 0) {
            initParams(paramCache);
        }
        if (PARAMS.containsKey(TRACE_VEHICLE_ID)) {
            final String traceVehicleId = (String) PARAMS.get(TRACE_VEHICLE_ID);
            if (!StringUtils.isBlank(traceVehicleId)) {
                final String message = "[" + TRACE_VEHICLE_ID + "]初始化为[" + traceVehicleId + "]";
                LOG.info(message);
            }
        }
    }

    public boolean isTraceVehicleId(String vehicleId) {
        if (StringUtils.isBlank(vehicleId)) {
            return false;
        }

        final String traceVehicleId = (String) PARAMS.get(TRACE_VEHICLE_ID);
        if (!StringUtils.isBlank(traceVehicleId)) {
            return vehicleId.matches(traceVehicleId);
        }
        return false;
    }

    public void autoLog(@NotNull String vehicleId, @NotNull Runnable func) {
        if (isTraceVehicleId(vehicleId)) {
            func.run();
        }
    }

    public <T> void autoLog(@NotNull String vehicleId, @NotNull T t, @NotNull BiConsumer<String, ? super T> func) {
        if (isTraceVehicleId(vehicleId)) {
            func.accept(vehicleId, t);
        }
    }

    private void initParams(Map<String, String> paramCache) {
        for (Map.Entry<String, String> entry : paramCache.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isBlank(key)
                    || StringUtils.isBlank(value)) {
            } else {

                if (org.apache.commons.lang.math.NumberUtils.isNumber(value)) {
                    // 缓存数字配置
                    Object val;
                    value = org.apache.commons.lang.math.NumberUtils.isNumber(value) ? value : "0";
                    if (value.contains(".")) {

                        val = Double.parseDouble(value);
                    } else if (value.length() < 9) {

                        val = Integer.parseInt(value);
                    } else {

                        val = Long.parseLong(value);
                    }
                    PARAMS.put(key, val);

                } else {
                    // 缓存非数字配置
                    PARAMS.put(key, value);
                }
            }
        }
    }

    /**
     * 重新从Redis读取数据构建参数
     */
    public void rebulid() {
        initParams();
    }
}
