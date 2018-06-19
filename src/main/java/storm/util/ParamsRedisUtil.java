package storm.util;

import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dao.DataToRedis;
import storm.system.SysDefine;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @author wza
 * 从Redis读取配置
 */
public final class ParamsRedisUtil {

    // region 静态区

    /**
     * 日志组件
     */
    private static final Logger logger = LoggerFactory.getLogger(ParamsRedisUtil.class);

    /**
     * SOC百分值
     */
    public static final String LT_ALARM_SOC_PERCENT = "lt.alarm.soc";
    /**
     * 闲置车辆判定, 达到闲置状态时长, 默认1天, 单位:秒
     */
    @SuppressWarnings("SpellCheckingInspection")
    public static final String GT_INIDLE_TIME_OUT_SECOND = "gt.inidle.timeOut.time";
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
    public static final String GPS_JUDGE_SECOND = "gps.judge.time";
    /**
     * 秒
     */
    public static final String CAN_JUDGE_SECOND = "can.judge.time";
    /**
     * 配置来源于Redis哪个库
     */
    private static final int DB_INDEX = 4;
    /**
     * 配置来自库中哪个键的Hash
     */
    private static final String CAL_QY_CONF = "cal.qy.conf";

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
		PARAMS.put(GT_INIDLE_TIME_OUT_SECOND, 60 * 60 * 24);
		PARAMS.put(GPS_NOVALUE_CONTINUE_NO, 5);
		PARAMS.put(GPS_HASVALUE_CONTINUE_NO, 10);
		PARAMS.put(CAN_NOVALUE_CONTINUE_NO, 5);
		PARAMS.put(CAN_HASVALUE_CONTINUE_NO, 10);
		PARAMS.put(MILE_HOP_NUM, 2);
		PARAMS.put(GPS_JUDGE_SECOND, 10800);
		PARAMS.put(CAN_JUDGE_SECOND, 10800);

		PARAMS.put(SysDefine.RULE_OVERRIDE, "default");
		PARAMS.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT, 3);
		PARAMS.put(SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND, 0);
		PARAMS.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT, 3);
		PARAMS.put(SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND, 0);
		PARAMS.put(SysDefine.NOTICE_TIME_RANGE_ABS_MILLISECOND, 1000 * 60 * 10);
		PARAMS.put(TRACE_VEHICLE_ID, "test\\d+");

        final Properties properties = ConfigUtils.sysDefine;
        if(properties != null) {
            // region 规则覆盖
            {
                final String ruleOverride = properties.getProperty(SysDefine.RULE_OVERRIDE);
                if (!ObjectUtils.isNullOrWhiteSpace(ruleOverride)) {
                    PARAMS.put(SysDefine.RULE_OVERRIDE, ruleOverride);
                }
            }
            // endregion
            // region 触发CAN故障需要的连续帧数
            {
                final String alarmCanFaultTriggerContinueCount = properties.getProperty(
                    SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT);
                if (!ObjectUtils.isNullOrWhiteSpace(alarmCanFaultTriggerContinueCount)) {
                    PARAMS.put(
                        SysDefine.NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT,
                        alarmCanFaultTriggerContinueCount);
                }
            }
            // endregion
            // region 触发CAN故障需要的持续时长
            {
                final String alarmCanFaultTriggerTimeoutMillisecond = properties.getProperty(
                    SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND);
                if (!ObjectUtils.isNullOrWhiteSpace(alarmCanFaultTriggerTimeoutMillisecond)) {
                    PARAMS.put(
                        SysDefine.NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND,
                        alarmCanFaultTriggerTimeoutMillisecond);
                }
            }
            // endregion
            // region 触发CAN正常需要的连续帧数
            {
                final String alarmCanNormalTriggerContinueCount = properties.getProperty(
                    SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT);
                if (!ObjectUtils.isNullOrWhiteSpace(alarmCanNormalTriggerContinueCount)) {
                    PARAMS.put(
                        SysDefine.NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT,
                        alarmCanNormalTriggerContinueCount);
                }
            }
            // endregion
            //region 触发CAN正常需要的持续时长
            {
                final String alarmCanNormalTriggerTimeoutMillisecond = properties.getProperty(
                    SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND);
                if (!ObjectUtils.isNullOrWhiteSpace(alarmCanNormalTriggerTimeoutMillisecond)) {
                    PARAMS.put(
                        SysDefine.NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND,
                        alarmCanNormalTriggerTimeoutMillisecond);
                }
            }
            // endregion
            //region 时间数值异常范围
            {
                final String noticeTimeRangeAbsMillisecond = properties.getProperty(
                    SysDefine.NOTICE_TIME_RANGE_ABS_MILLISECOND);
                if (!ObjectUtils.isNullOrWhiteSpace(noticeTimeRangeAbsMillisecond)) {
                    PARAMS.put(
                        SysDefine.NOTICE_TIME_RANGE_ABS_MILLISECOND,
                        noticeTimeRangeAbsMillisecond);
                }
            }
            // endregion
        }
	}
	
	private void initParams() {
        Map<String, String> paramCache = redis.getMap(DB_INDEX, CAL_QY_CONF);
        if (null != paramCache && paramCache.size() > 0) {
            initParams(paramCache);
        }
        if(PARAMS.containsKey(TRACE_VEHICLE_ID)) {
            final String traceVehicleId = (String) PARAMS.get(TRACE_VEHICLE_ID);
            if (!ObjectUtils.isNullOrWhiteSpace(traceVehicleId)) {
                final String message = "[" + TRACE_VEHICLE_ID + "]初始化为[" + traceVehicleId + "]";
                logger.info(message);
            }
        }
	}

	public boolean isTraceVehicleId(String vehicleId) {
	    if(ObjectUtils.isNullOrWhiteSpace(vehicleId)) {
	        return false;
        }

        final String traceVehicleId = (String)PARAMS.get(TRACE_VEHICLE_ID);
        if(!ObjectUtils.isNullOrWhiteSpace(traceVehicleId)) {
            return vehicleId.matches(traceVehicleId);
        }
        return false;
    }

	private void initParams(Map<String, String> paramCache) {
        for (Map.Entry<String, String> entry : paramCache.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (ObjectUtils.isNullOrWhiteSpace(key)
                || ObjectUtils.isNullOrWhiteSpace(value)) {
            } else {

                if (NumberUtils.stringIsNumber(value)) {
                    // 缓存数字配置
                    Object val;
                    value = NumberUtils.stringNumber(value);
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
	public void rebulid(){
		initParams();
	}

	public static void main(String[] args) {
        final Map<String, String> paramCache = new TreeMap<>();
        paramCache.put("lt.alarm.soc", "10");
        paramCache.put("gt.inidle.timeOut.time", "120");
        paramCache.put("gps.novalue.continue.no", "10");
        paramCache.put("can.novalue.continue.no", "10");
        paramCache.put("mile.hop.num", "2");
        paramCache.put("gps.judge.time", "10800");
        paramCache.put("can.judge.time", "10800");

        final ParamsRedisUtil instance = ParamsRedisUtil.getInstance();
        instance.initParams(paramCache);
    }
}
