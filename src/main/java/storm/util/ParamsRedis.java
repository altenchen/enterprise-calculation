package storm.util;

import java.util.HashMap;
import java.util.Map;

import storm.dao.DataToRedis;

public class ParamsRedis {

	public final static Map<String, Object> PARAMS = new HashMap<String, Object>();
	private static DataToRedis redis;
	static {
		sysDefault();
		redis = new DataToRedis();
		initParams();
	}
	
	static void sysDefault() {
		PARAMS.put("lt.alarm.soc", 10);
		PARAMS.put("gt.inidle.timeOut.time", 86400);
		PARAMS.put("gps.novalue.continue.no", 10);
		PARAMS.put("can.novalue.continue.no", 10);
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
