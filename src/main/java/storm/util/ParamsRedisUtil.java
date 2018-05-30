package storm.util;

import java.util.HashMap;
import java.util.Map;

import storm.dao.DataToRedis;

public class ParamsRedisUtil {

	public final static Map<String, Object> PARAMS = new HashMap<String, Object>();
	private static DataToRedis redis;
	static {
		sysDefault();
		redis = new DataToRedis();
		initParams();
	}
	
	static void sysDefault() {
		PARAMS.put("lt.alarm.soc", 10);//百分值
		PARAMS.put("gt.inidle.timeOut.time", 86400);//秒
		PARAMS.put("gps.novalue.continue.no", 5);//连续多少帧没有gps，算为未定位车辆
		PARAMS.put("gps.hasvalue.continue.no", 10);//连续多少帧有gps，算为定位车辆
		PARAMS.put("can.novalue.continue.no", 5);//连续多少帧没有can状态，算为无can状态车辆
		PARAMS.put("can.hasvalue.continue.no", 10);//连续多少帧有can状态，算为有can状态车辆
		PARAMS.put("mile.hop.num", 2);//里程跳变数
		PARAMS.put("gps.judge.time", 10800);//秒
		PARAMS.put("can.judge.time", 10800);//秒
	}
	
	static void initParams() {
		
		try {
			if (null == redis) {
				redis = new DataToRedis();
			}
			
			Map<String, String> paramMap = redis.getMap(4, "cal.qy.conf");
			if (null != paramMap && paramMap.size() > 0) {
				for (Map.Entry<String, String> entry : paramMap.entrySet()) {
					String key = entry.getKey();
					String value = entry.getValue();
					if (null !=key && !"".equals(key.trim())
							&& null != value && !"".equals(value.trim())) {
						
						boolean num = NumberUtils.stringIsNumber(value);
						if (num) {
							
							Object val = 0;
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
							
							PARAMS.put(key, value);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void rebulid(){
		initParams();
	}
	
	
}
