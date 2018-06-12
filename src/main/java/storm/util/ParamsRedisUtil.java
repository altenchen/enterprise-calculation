package storm.util;

import java.util.HashMap;
import java.util.Map;

import storm.dao.DataToRedis;

/**
 * @author wza
 * 从Redis读取配置
 */
public class ParamsRedisUtil {

    /**
     * 参数缓存
     */
	public static final Map<String, Object> PARAMS = new HashMap<>();

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
    private static final DataToRedis redis = new DataToRedis();
	static {
		resetToDefault();
        initParams();
	}

    /**
     * 设置默认值
     */
	private static void resetToDefault() {
		PARAMS.put(LT_ALARM_SOC_PERCENT, 10);
		PARAMS.put(GT_INIDLE_TIME_OUT_SECOND, 60 * 60 * 24);
		PARAMS.put(GPS_NOVALUE_CONTINUE_NO, 5);
		PARAMS.put(GPS_HASVALUE_CONTINUE_NO, 10);
		PARAMS.put(CAN_NOVALUE_CONTINUE_NO, 5);
		PARAMS.put(CAN_HASVALUE_CONTINUE_NO, 10);
		PARAMS.put(MILE_HOP_NUM, 2);
		PARAMS.put(GPS_JUDGE_SECOND, 10800);
		PARAMS.put(CAN_JUDGE_SECOND, 10800);
	}
	
	private static void initParams() {
		try {
			Map<String, String> paramCache = redis.getMap(DB_INDEX, CAL_QY_CONF);
			if (null != paramCache && paramCache.size() > 0) {
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    /**
     * 重新从Redis读取数据构建参数
     */
	public static void rebulid(){
		initParams();
	}
	
	public static String getGT_INIDLE_TIME_OUT_SECOND() {
	    return (String)PARAMS.get(GT_INIDLE_TIME_OUT_SECOND);
    }
}
