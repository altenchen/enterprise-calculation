package storm.handler.cusmade;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.fastjson.JSON;

import storm.cache.SysRealDataCache;
import storm.dao.DataToRedis;
import storm.dto.FillChargeCar;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.service.TimeFormatService;
import storm.system.ProtocolItem;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.GpsUtil;
import storm.util.NumberUtils;
import storm.util.ObjectUtils;
import storm.util.ParamsRedis;
import storm.util.UUIDUtils;

/**
 * <p>
 * 临时处理 沃特玛的需求 处理简单实现方法
 * 后续抽象需求出来
 * 用其他通用的工具来代替
 * </p>
 * @author 76304
 *
 */
public class CarRulehandler implements InfoNotice{

	private Map<String, Integer> vidnogps;
	private Map<String, Integer> vidnormgps;
	
	private Map<String, Integer> vidnocan;
	private Map<String, Integer> vidnormcan;
	
	private Map<String, Integer> vidIgnite;
	private Map<String, Integer> vidShut;
	private Map<String, Double> igniteShutMaxSpeed;
	private Map<String, Double> lastSoc;
	private Map<String, Double> lastMile;
	
	private Map<String, Integer> vidlowsoc;
	private Map<String, Integer> vidnormsoc;
	
	private Map<String, Integer> vidSpeedGtZero;
	private Map<String, Integer> vidSpeedZero;
	
	private Map<String, Integer> vidFlySt;
	private Map<String, Integer> vidFlyEd;
	
	private Map<String, Map<String, Object>> vidsocNotice;
	private Map<String, Map<String, Object>> vidcanNotice;
	private Map<String, Map<String, Object>> vidIgniteShutNotice;
	private Map<String, Map<String, Object>> vidgpsNotice;
	private Map<String, Map<String, Object>> vidSpeedGtZeroNotice;
	private Map<String, Map<String, Object>> vidFlyNotice;
	private Map<String, Map<String, Object>> vidOnOffNotice;
	private Map<String, Long> lastTime;
	
	DataToRedis redis;
	private Recorder recorder;
	static String onOffRedisKeys;
	
	static TimeFormatService timeformat;
	static int topn;
	static long offlinetime = 600000;//600秒
	static int socAlarm = 10;//低于10%
	
	static int db =6;
	static int socRule = 0;//1代表规则启用
	static int canRule = 0;//1代表规则启用
	static int igniteRule = 0;//1代表规则启用
	static int gpsRule = 0;//1代表规则启用
	static int abnormalRule = 0;//1代表规则启用
	static int flyRule = 0;//1代表规则启用
	static int onoffRule = 0;//1代表规则启用
	
	static {
		timeformat = new TimeFormatService();
		topn = 20;
		onOffRedisKeys = "vehCache.qy.onoff.notice";
		if (null != ConfigUtils.sysDefine) {
			String off = ConfigUtils.sysDefine.getProperty("redis.offline.time");
			if (!ObjectUtils.isNullOrEmpty(off)) {
				offlinetime = Long.parseLong(off)*1000;
			}
			
			String value = ConfigUtils.sysDefine.getProperty("sys.soc.rule");
			if (!ObjectUtils.isNullOrEmpty(value)) {
				socRule = Integer.parseInt(value);
				value = null;
			}
			
			value = ConfigUtils.sysDefine.getProperty("sys.can.rule");
			if (!ObjectUtils.isNullOrEmpty(value)) {
				canRule = Integer.parseInt(value);
				value = null;
			}
			
			value = ConfigUtils.sysDefine.getProperty("sys.ignite.rule");
			if (!ObjectUtils.isNullOrEmpty(value)) {
				igniteRule = Integer.parseInt(value);
				value = null;
			}
			
			value = ConfigUtils.sysDefine.getProperty("sys.gps.rule");
			if (!ObjectUtils.isNullOrEmpty(value)) {
				gpsRule = Integer.parseInt(value);
				value = null;
			}
			
			value = ConfigUtils.sysDefine.getProperty("sys.abnormal.rule");
			if (!ObjectUtils.isNullOrEmpty(value)) {
				abnormalRule = Integer.parseInt(value);
				value = null;
			}
			
			value = ConfigUtils.sysDefine.getProperty("sys.fly.rule");
			if (!ObjectUtils.isNullOrEmpty(value)) {
				flyRule = Integer.parseInt(value);
				value = null;
			}
			
			value = ConfigUtils.sysDefine.getProperty("sys.onoff.rule");
			if (!ObjectUtils.isNullOrEmpty(value)) {
				onoffRule = Integer.parseInt(value);
				value = null;
			}
			
		}
		Object socVal = ParamsRedis.PARAMS.get("lt.alarm.soc");
		if (null != socVal) {
			socAlarm = (int)socVal;
		}
	}
	{
		vidnogps = new HashMap<String, Integer>();
		vidnormgps = new HashMap<String, Integer>();
		vidnocan = new HashMap<String, Integer>();
		vidnormcan = new HashMap<String, Integer>();
		vidIgnite = new HashMap<String, Integer>();
		vidShut = new HashMap<String, Integer>();
		igniteShutMaxSpeed = new HashMap<String, Double>();
		lastMile = new HashMap<String, Double>();
		lastSoc = new HashMap<String, Double>();
		vidlowsoc = new HashMap<String, Integer>();
		vidnormsoc = new HashMap<String, Integer>();
		vidsocNotice = new HashMap<String, Map<String, Object>>();
		vidcanNotice = new HashMap<String, Map<String, Object>>();
		vidIgniteShutNotice = new HashMap<String, Map<String, Object>>();
		vidgpsNotice = new HashMap<String, Map<String, Object>>();
		
		vidSpeedGtZero = new HashMap<String, Integer>();
		vidSpeedZero = new HashMap<String, Integer>();
		vidSpeedGtZeroNotice = new HashMap<String, Map<String, Object>>();
		
		vidFlySt = new HashMap<String, Integer>();
		vidFlyEd = new HashMap<String, Integer>();
		vidFlyNotice = new HashMap<String, Map<String, Object>>();
		vidOnOffNotice = new HashMap<String, Map<String, Object>>();
		lastTime = new HashMap<String, Long>();
		redis = new DataToRedis();
		recorder = new RedisRecorder(redis);
		restartInit(true);
	}
	@Override
	public Map<String, Object> genotice(Map<String, String> dat) {
		
		return null;
	}

	@Override
	public List<Map<String, Object>> genotices(Map<String, String> dat) {
		if (ObjectUtils.isNullOrEmpty(dat)
				|| !dat.containsKey(ProtocolItem.VID)
				|| !dat.containsKey(ProtocolItem.TIME)) {
			return null;
		}
		
		String vid = dat.get(ProtocolItem.VID);
		if (ObjectUtils.isNullOrEmpty(vid)
				|| ObjectUtils.isNullOrEmpty(dat.get(ProtocolItem.TIME))) {
			return null;
		}
		
		lastTime.put(vid, System.currentTimeMillis());
		List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();
		
		List<Map<String, Object>> socjudges = null;
		Map<String, Object> canjudge = null;
		Map<String, Object> ignite = null;
		Map<String, Object> gpsjudge = null;
		Map<String, Object> abnormaljudge = null;
		Map<String, Object> flyjudge = null;
		Map<String, Object> onoff = null;
		
		if (1 == socRule)
			socjudges = lowsoc(dat);
		
		if (1 == canRule)
			canjudge = nocan(dat);
		
		if (1 == igniteRule)
			ignite = igniteShut(dat);
		
		if (1 == gpsRule)
			gpsjudge = nogps(dat);
		
		if (1 == abnormalRule)
			abnormaljudge = abnormalCar(dat);
		
		if (1 == flyRule)
			flyjudge = flySe(dat);
		
		if (1 == onoffRule)
			onoff = onOffline(dat);
		
		if (! ObjectUtils.isNullOrEmpty(socjudges)) {
			list.addAll(socjudges);
		}
		if (! ObjectUtils.isNullOrEmpty(canjudge)) {
			list.add(canjudge);
		}
		if (! ObjectUtils.isNullOrEmpty(ignite)) {
			list.add(ignite);
		}
		if (! ObjectUtils.isNullOrEmpty(gpsjudge)) {
			list.add(gpsjudge);
		}
		if (! ObjectUtils.isNullOrEmpty(abnormaljudge)) {
			list.add(abnormaljudge);
		}
		if (! ObjectUtils.isNullOrEmpty(flyjudge)) {
			list.add(flyjudge);
		}
		if (! ObjectUtils.isNullOrEmpty(onoff)) {
			list.add(onoff);
		}
		if (list.size()>0) {
			return list;
		}
		return null;
	}
	
	//soc<30 后面抽象成通用的规则
	/**
	 * soc 过低
	 */
	private List<Map<String, Object>> lowsoc(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
			String vid = dat.get(ProtocolItem.VID);
			String time = dat.get(ProtocolItem.TIME);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)) {
				return null;
			}
			String soc = dat.get(ProtocolItem.SOC);
			String latit = dat.get(ProtocolItem.latitude);
			String longi = dat.get(ProtocolItem.longitude);
			String location = longi+","+latit;
			String noticetime = timeformat.toDateString(new Date());
			List<Map<String, Object>> noticeMsgs = new LinkedList<Map<String, Object>>();
			if (! ObjectUtils.isNullOrEmpty(soc)) {
				double socNum = Double.parseDouble(NumberUtils.stringNumber(soc));
				if (socNum < socAlarm) {
					int cnts = 0;
					if (vidlowsoc.containsKey(vid)) {
						cnts = vidlowsoc.get(vid);
					}
					cnts++;
					vidlowsoc.put(vid, cnts);
					if (cnts >=10) {
						
						Map<String, Object> notice = vidsocNotice.get(vid);
						if (null == notice) {
							notice =  new TreeMap<String, Object>();
							notice.put("msgType", "SOC_ALARM");
							notice.put("msgId", UUIDUtils.getUUID());
							notice.put("vid", vid);
							notice.put("stime", time);
							notice.put("ssoc", socNum);
							notice.put("count", cnts);
							notice.put("status", 1);
							notice.put("location", location);
						}else{
							notice.put("count", cnts);
							notice.put("status", 2);
							notice.put("location", location);
						}
						notice.put("noticetime", noticetime);
						vidsocNotice.put(vid, notice);
						//查找附近补电车 并且存入redis缓存 2库
						try {
							double longitude = Double.parseDouble(NumberUtils.stringNumber(longi));
							double latitude = Double.parseDouble(NumberUtils.stringNumber(latit));
							longitude = longitude/1000000.0;
							latitude = latitude/1000000.0;
							Map<String, Object> chargeMap = chargeCarNotice(vid, longitude, latitude);
							if (null != chargeMap && chargeMap.size() >0) {
								noticeMsgs.add(chargeMap);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						if(1 == (int)notice.get("status"))
							noticeMsgs.add(notice);
						return noticeMsgs;
					}
				} else {
					if (vidlowsoc.containsKey(vid)){
						int cnts = 0;
						if (vidnormsoc.containsKey(vid)) {
							cnts = vidnormsoc.get(vid);
						}
						cnts++;
						vidnormsoc.put(vid, cnts);
						
						if (cnts >=10) {
							Map<String, Object> notice = vidsocNotice.get(vid);
							vidlowsoc.remove(vid);
							vidsocNotice.remove(vid);
							if (null != notice) {
								notice.put("status", 3);
								notice.put("location", location);
								notice.put("etime", time);
								notice.put("esoc", socNum);
								notice.put("noticetime", noticetime);
								noticeMsgs.add(notice);
								return noticeMsgs;
							}
						}
					
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 查找附近补电车 保存到 redis 中
	 * @param vid
	 * @param longitude
	 * @param latitude
	 */
	private Map<String, Object> chargeCarNotice(String vid,double longitude,double latitude){
		Map<String,FillChargeCar> fillvidgps = SysRealDataCache.chargeCars();
		Map<Double, FillChargeCar> chargeCarInfo = findNearFill(longitude, latitude, fillvidgps);
		
		if (null!= chargeCarInfo) {
			Map<String, Object> chargeMap = new TreeMap<String, Object>();
			List<Map<String, Object>> chargeCars = new LinkedList<Map<String, Object>>();
			Map<String, String> topnCars = new TreeMap<String, String>();
			int cts = 0;
			for (Map.Entry<Double, FillChargeCar> entry : chargeCarInfo.entrySet()) {
				cts++;
				if (cts > topn) {
					break;
				}
				double distance = entry.getKey();
				FillChargeCar chargeCar = entry.getValue();
				
				//save to redis map
				Map<String, Object> jsonMap = new TreeMap<String, Object>();
				jsonMap.put("vid", chargeCar.vid);
				jsonMap.put("longitude", chargeCar.longitude);
				jsonMap.put("latitude", chargeCar.latitude);
				jsonMap.put("lastOnline", chargeCar.lastOnline);
				jsonMap.put("distance", distance);
				
				String jsonString = JSON.toJSONString(jsonMap);
				topnCars.put(""+cts, jsonString);
				//send to kafka map
				Map<String, Object> kMap = new TreeMap<String, Object>();
				kMap.put("vid", chargeCar.vid);
				kMap.put("location", chargeCar.longitude+","+chargeCar.latitude);
				kMap.put("lastOnline", chargeCar.lastOnline);
				kMap.put("gpsDis", distance);
				kMap.put("ranking", cts);
				kMap.put("running", chargeCar.running);
				
				chargeCars.add(kMap);
			}
			
			if (topnCars.size() > 0) {
				redis.saveMap(topnCars, 2, "charge-car-"+vid);
			}
			if (chargeCars.size() > 0) {
				
				chargeMap.put("vid", vid);
				chargeMap.put("msgType", "CHARGE_CAR_NOTICE");
				chargeMap.put("location", longitude*1000000+","+latitude*1000000);
				chargeMap.put("fillChargeCars", chargeCars);
				return chargeMap;
			}
		}
		return null;
	}
	/**
	 * 查找附件补电车
	 * @param longitude 经度
	 * @param latitude 纬度
	 * @param fillvidgps 缓存的补电车 vid [经度，纬度]
	 */
	private Map<Double, FillChargeCar>  findNearFill(double longitude,double latitude,Map<String,FillChargeCar> fillvidgps){
		//
		if (null == fillvidgps || fillvidgps.size() == 0) {
			return null;
		}
		
		if ((0 == longitude && 0 == latitude )
				|| Math.abs(longitude) > 180
				|| Math.abs(latitude) > 180) {
			return null;
		}
		/**
		 * 按照此种方式加上远近排名的话可能会有 bug,
		 * 此方法假设 任意两点的 车子gps距离 的double值不可能相等
		 * 出现的 概率极低，暂时忽略
		 */
		
		Map<Double, FillChargeCar> carSortMap = new TreeMap<Double, FillChargeCar>();
		
		for (Map.Entry<String, FillChargeCar> entry : fillvidgps.entrySet()) {
//			String fillvid = entry.getKey();
			FillChargeCar chargeCar = entry.getValue();
			double distance = GpsUtil.getDistance(longitude, latitude, chargeCar.longitude, chargeCar.latitude);
			carSortMap.put(distance, chargeCar);
		}
		if (carSortMap.size()>0) {
			return carSortMap;
		}
		return null;
	}
	/**
	 * 无can车辆
	 */
	private Map<String, Object> nocan(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
			String vid = dat.get(ProtocolItem.VID);
			String time = dat.get(ProtocolItem.TIME);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)) {
				return null;
			}
			String canList = dat.get(ProtocolItem.CAN_LIST);
			String latit = dat.get(ProtocolItem.latitude);
			String longi = dat.get(ProtocolItem.longitude);
			String location = longi+","+latit;
			String noticetime = timeformat.toDateString(new Date());
			if (ObjectUtils.isNullOrEmpty(canList)) {
				int cnts = 0;
				if (vidnocan.containsKey(vid)) {
					cnts = vidnocan.get(vid);
				}
				cnts++;
				vidnocan.put(vid, cnts);
				if (cnts >=10) {
					
					Map<String, Object> notice = vidcanNotice.get(vid);
					if (null == notice) {
						notice =  new TreeMap<String, Object>();
						notice.put("msgType", "NO_CAN_VEH");
						notice.put("vid", vid);
						notice.put("msgId", UUIDUtils.getUUID());
						notice.put("stime", time);
						notice.put("count", cnts);
						notice.put("status", 1);
						notice.put("location", location);
					}else{
						notice.put("count", cnts);
						notice.put("status", 2);
						notice.put("location", location);
					}
					notice.put("noticetime", noticetime);
					vidcanNotice.put(vid, notice);
					
					if(1 == (int)notice.get("status"))
						return notice;
				}
			}else {
				if (vidnocan.containsKey(vid)){
					int cnts = 0;
					if (vidnormcan.containsKey(vid)) {
						cnts = vidnormcan.get(vid);
					}
					cnts++;
					vidnormcan.put(vid, cnts);
					
					if (cnts >=10) {
						Map<String, Object> notice = vidcanNotice.get(vid);
						vidnocan.remove(vid);
						vidcanNotice.remove(vid);
						if (null != notice) {
							notice.put("status", 3);
							notice.put("location", location);
							notice.put("etime", time);
							notice.put("noticetime", noticetime);
							return notice;
						}
					}
				
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * IGNITE_SHUT_MESSAGE
	 * 点火熄火
	 * @param dat
	 * @return
	 */
	private Map<String, Object> igniteShut(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
			String vid = dat.get(ProtocolItem.VID);
			String vin = dat.get(ProtocolItem.VIN);
			String time = dat.get(ProtocolItem.TIME);
			String carStatus = dat.get(ProtocolItem.CAR_STATUS);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)
					|| ObjectUtils.isNullOrEmpty(carStatus)) {
				return null;
			}
			
			String latit = dat.get(ProtocolItem.latitude);
			String longi = dat.get(ProtocolItem.longitude);
			String location = longi+","+latit;
			String noticetime = timeformat.toDateString(new Date());
			String speed = dat.get(ProtocolItem.SPEED);
			String socStr = dat.get(ProtocolItem.SOC);
			String mileageStr = dat.get(ProtocolItem.TOTAL_MILEAGE);
			
			double soc = -1;
			if (!ObjectUtils.isNullOrEmpty(socStr)) {
				soc = Double.parseDouble(NumberUtils.stringNumber(socStr));
				if (-1 != soc) {
					
					lastSoc.put(vid, soc);
				}
			} else {
				if (lastSoc.containsKey(vid)) {
					soc = lastSoc.get(vid);
				}
			}
			double mileage = -1;
			if (!ObjectUtils.isNullOrEmpty(mileageStr)) {
				mileage = Double.parseDouble(NumberUtils.stringNumber(mileageStr));
				if (-1 != mileage) {
					
					lastMile.put(vid, mileage);
				}
			} else {
				if (lastMile.containsKey(vid)) {
					mileage = lastMile.get(vid);
				}
			}
			
			double maxSpd = -1;
			if ("1".equals(carStatus) 
					||"2".equals(carStatus)) {
				double spd = -1;
				if (!igniteShutMaxSpeed.containsKey(vid)) {
					igniteShutMaxSpeed.put(vid, maxSpd);
				}
				if (!ObjectUtils.isNullOrEmpty(speed)) {
					
					maxSpd = igniteShutMaxSpeed.get(vid);
					spd = Double.parseDouble(NumberUtils.stringNumber(speed));
					if (spd > maxSpd) {
						maxSpd = spd;
						igniteShutMaxSpeed.put(vid, maxSpd);
					}
				}
				
			}
			if ("1".equals(carStatus)) {//是否点火
				int cnts = 0;
				if (vidIgnite.containsKey(vid)) {
					cnts = vidIgnite.get(vid);
				}
				cnts++;
				vidIgnite.put(vid, cnts);
				if (vidShut.containsKey(vid)) {
					vidShut.remove(vid);
				}
				if (cnts >=2) {
					
					Map<String, Object> notice = vidIgniteShutNotice.get(vid);
					if (null == notice) {
						notice =  new TreeMap<String, Object>();
						notice.put("msgType", "IGNITE_SHUT_MESSAGE");
						notice.put("vid", vid);
						notice.put("vin", vin);
						notice.put("msgId", UUIDUtils.getUUID());
						notice.put("stime", time);
						notice.put("soc", soc);
						notice.put("ssoc", soc);
						notice.put("mileage", mileage);
						notice.put("status", 1);
						notice.put("location", location);
					}else{
						double ssoc = (double)notice.get("ssoc");
						double energy = Math.abs(ssoc-soc);
						notice.put("soc", soc);
						notice.put("mileage", mileage);
						notice.put("maxSpeed", maxSpd);
						notice.put("energy", energy);
						notice.put("status", 2);
						notice.put("location", location);
					}
					notice.put("noticetime", noticetime);
					vidIgniteShutNotice.put(vid, notice);
					
					if(1 == (int)notice.get("status"))
						return notice;
				}
			} else if("2".equals(carStatus)) {//是否熄火
				if (vidIgnite.containsKey(vid)){
					int cnts = 0;
					if (vidShut.containsKey(vid)) {
						cnts = vidShut.get(vid);
					}
					cnts++;
					vidShut.put(vid, cnts);
					
					if (cnts >=1) {
						Map<String, Object> notice = vidIgniteShutNotice.get(vid);
						vidIgnite.remove(vid);
						vidIgniteShutNotice.remove(vid);
						
						if (null != notice) {
							double ssoc = (double)notice.get("ssoc");
							double energy = Math.abs(ssoc-soc);
							notice.put("soc", soc);
							notice.put("mileage", mileage);
							notice.put("maxSpeed", maxSpd);
							notice.put("energy", energy);
							notice.put("status", 3);
							notice.put("location", location);
							notice.put("etime", time);
							notice.put("noticetime", noticetime);
							vidShut.remove(vid);
							return notice;
						}
					}
					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	private Map<String, Object> flySe(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
			String vid = dat.get(ProtocolItem.VID);
			String time = dat.get(ProtocolItem.TIME);
			String speed = dat.get(ProtocolItem.SPEED);
			String rev = dat.get(ProtocolItem.DRIVING_ELE_MAC_REV);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)
					|| ObjectUtils.isNullOrEmpty(speed)
					|| ObjectUtils.isNullOrEmpty(rev)) {
				return null;
			}
			double spd = Double.parseDouble(NumberUtils.stringNumber(speed));
			double rv = Double.parseDouble(NumberUtils.stringNumber(rev));
			String latit = dat.get(ProtocolItem.latitude);
			String longi = dat.get(ProtocolItem.longitude);
			String location = longi+","+latit;
			String noticetime = timeformat.toDateString(new Date());
			if (spd > 0 && rv>0) {
				
				int cnts = 0;
				if (vidFlySt.containsKey(vid)) {
					cnts = vidFlySt.get(vid);
				}
				cnts++;
				vidFlySt.put(vid, cnts);
				if (cnts >=10) {
					
					Map<String, Object> notice = vidFlyNotice.get(vid);
					if (null == notice) {
						notice =  new TreeMap<String, Object>();
						notice.put("msgType", "FLY_RECORD");
						notice.put("vid", vid);
						notice.put("msgId", UUIDUtils.getUUID());
						notice.put("stime", time);
						notice.put("count", cnts);
						notice.put("status", 1);
						notice.put("location", location);
					}else{
						notice.put("count", cnts);
						notice.put("status", 2);
						notice.put("location", location);
					}
					notice.put("noticetime", noticetime);
					vidFlyNotice.put(vid, notice);
					
					if(1 == (int)notice.get("status"))
						return notice;
				}
			}else {
				if (vidFlySt.containsKey(vid)){
					int cnts = 0;
					if (vidFlyEd.containsKey(vid)) {
						cnts = vidFlyEd.get(vid);
					}
					cnts++;
					vidFlyEd.put(vid, cnts);
					
					if (cnts >=10) {
						Map<String, Object> notice = vidFlyNotice.get(vid);
						vidFlySt.remove(vid);
						vidFlyNotice.remove(vid);
						if (null != notice) {
							notice.put("status", 3);
							notice.put("location", location);
							notice.put("etime", time);
							notice.put("noticetime", noticetime);
							return notice;
						}
					}
					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 北汽异常用车
	 * @param dat
	 * @return
	 */
	private Map<String, Object> abnormalCar(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
			String vid = dat.get(ProtocolItem.VID);
			String time = dat.get(ProtocolItem.TIME);
			String speed = dat.get(ProtocolItem.SPEED);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)
					|| ObjectUtils.isNullOrEmpty(speed)) {
				return null;
			}
			String latit = dat.get(ProtocolItem.latitude);
			String longi = dat.get(ProtocolItem.longitude);
			String location = longi+","+latit;
			String noticetime = timeformat.toDateString(new Date());
			long msgtime = timeformat.stringTimeLong(time);
			double spd = Double.parseDouble(NumberUtils.stringNumber(speed));
			if (spd > 0) {
				int cnts = 0;
				if (vidSpeedGtZero.containsKey(vid)) {
					cnts = vidSpeedGtZero.get(vid);
				}
				cnts++;
				vidSpeedGtZero.put(vid, cnts);
				if (cnts >=1) {
					
					Map<String, Object> notice = vidSpeedGtZeroNotice.get(vid);
					if (null == notice) {
						notice =  new TreeMap<String, Object>();
						notice.put("msgType", "ABNORMAL_USE_VEH");
						notice.put("vid", vid);
						notice.put("msgId", UUIDUtils.getUUID());
						notice.put("stime", time);
						notice.put("count", cnts);
						notice.put("status", 1);
						notice.put("location", location);
					}else{
						long stime = timeformat.stringTimeLong((String)notice.get("stime"));
						long timespace = msgtime - stime;
						if ("true".equals(notice.get("isNotice"))) {
							notice.put("count", cnts);
							notice.put("status", 2);
							notice.put("location", location);
						} else {
							if (timespace >= 150000) {
								notice.put("utime", timespace/1000);
								notice.put("status", 1);
								notice.put("isNotice", "true");
								notice.put("noticetime", noticetime);
								vidSpeedGtZeroNotice.put(vid, notice);
								return notice;
							}
						}
					}
					notice.put("noticetime", noticetime);
					vidSpeedGtZeroNotice.put(vid, notice);
					
//					if(1 == (int)notice.get("status"))
//						return notice;
				}
			}else {
				if (vidSpeedGtZero.containsKey(vid)){
					int cnts = 0;
					if (vidSpeedZero.containsKey(vid)) {
						cnts = vidSpeedZero.get(vid);
					}
					cnts++;
					vidSpeedZero.put(vid, cnts);
					
					if (cnts >=10) {
						Map<String, Object> notice = vidSpeedGtZeroNotice.get(vid);
						vidSpeedGtZero.remove(vid);
						vidSpeedGtZeroNotice.remove(vid);
						if (null != notice) {
							notice.put("status", 3);
							notice.put("location", location);
							notice.put("etime", time);
							notice.put("noticetime", noticetime);
							return notice;
						}
					}
					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 未定位车辆
	 */
	Map<String, Object> nogps(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
			String vid = dat.get(ProtocolItem.VID);
			String time = dat.get(ProtocolItem.TIME);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)) {
				return null;
			}
			String latit = dat.get(ProtocolItem.latitude);
			String longi = dat.get(ProtocolItem.longitude);
			String noticetime = timeformat.toDateString(new Date());
			
			boolean isValid = true;
			if (! ObjectUtils.isNullOrEmpty(latit)
					&& !ObjectUtils.isNullOrEmpty(longi)) {
				latit = NumberUtils.stringNumber(latit);
				longi = NumberUtils.stringNumber(longi);
				double latitd = Double.parseDouble(latit);
				double longid = Double.parseDouble(longi);
				if (latitd > 180000000 
						|| longid > 180000000) {
					isValid = false;
				}
			}
			if (! isValid
					|| ObjectUtils.isNullOrEmpty(latit)
					|| ObjectUtils.isNullOrEmpty(longi)) {
				
				int cnts = 0;
				if (vidnogps.containsKey(vid)) {
					cnts = vidnogps.get(vid);
				}
				cnts++;
				vidnogps.put(vid, cnts);
				if (cnts >=10) {
					
					Map<String, Object> notice = vidgpsNotice.get(vid);
					if (null == notice) {
						notice =  new TreeMap<String, Object>();
						notice.put("msgType", "NO_POSITION_VEH");
						notice.put("vid", vid);
						notice.put("msgId", UUIDUtils.getUUID());
						notice.put("stime", time);
						notice.put("count", cnts);
						notice.put("status", 1);
					}else{
						notice.put("count", cnts);
						notice.put("status", 2);
					}
					notice.put("noticetime", noticetime);
					vidgpsNotice.put(vid, notice);
					if(1 == (int)notice.get("status"))
						return notice;
				}
			}else {
				if (vidnogps.containsKey(vid)){
					int cnts = 0;
					if (vidnormgps.containsKey(vid)) {
						cnts = vidnormgps.get(vid);
					}
					cnts++;
					vidnormgps.put(vid, cnts);
					
					if (cnts >=10) {
						Map<String, Object> notice = vidgpsNotice.get(vid);
						vidnogps.remove(vid);
						vidgpsNotice.remove(vid);
						if (null != notice) {
							String location = longi+","+latit;
							notice.put("status", 3);
							notice.put("location", location);
							notice.put("etime", time);
							notice.put("noticetime", noticetime);
							return notice;
						}
					}
				
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private Map<String, Object> onOffline(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
			String vid = dat.get(ProtocolItem.VID);
			String vin = dat.get(ProtocolItem.VIN);
			String time = dat.get(ProtocolItem.TIME);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)
					|| ObjectUtils.isNullOrEmpty(vin)) {
				return null;
			}
			
			boolean isoff = isOffline(dat);
			if (!isoff) {//上线
				Map<String, Object> notice = vidOnOffNotice.get(vid);
				if (null == notice) {
					String noticetime = timeformat.toDateString(new Date());
					notice =  new TreeMap<String, Object>();
					notice.put("msgType", "ON_OFF");
					notice.put("vid", vid);
					notice.put("vin", vin);
					notice.put("msgId", UUIDUtils.getUUID());
					notice.put("stime", time);
					notice.put("status", 1);
					notice.put("noticetime", noticetime);
				}else{
					notice.put("status", 2);
				}
				vidOnOffNotice.put(vid, notice);
				
				if(1 == (int)notice.get("status")){
					recorder.save(db, onOffRedisKeys,vid, notice);
					return notice;
				}
			} else {//是否离线
				if (vidOnOffNotice.containsKey(vid)){
					
					Map<String, Object> notice = vidOnOffNotice.get(vid);
					if (null != notice) {
						String noticetime = timeformat.toDateString(new Date());
						notice.put("status", 3);
						notice.put("etime", time);
						notice.put("noticetime", noticetime);
						vidOnOffNotice.remove(vid);
						recorder.del(db, onOffRedisKeys, vid);
						return notice;
					}
					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private boolean isOffline(Map<String, String> dat){
		String msgType = dat.get(SysDefine.MESSAGETYPE);
		if (SysDefine.LOGIN.equals(msgType)) {
			String type = dat.get(ProtocolItem.REG_TYPE);
			if ("1".equals(type)){
				return false;
			} else if ("2".equals(type)){
				return true;
			} else {
				String logoutSeq = dat.get(ProtocolItem.LOGOUT_SEQ);
				String loginSeq = dat.get(ProtocolItem.LOGIN_SEQ);
				if (! ObjectUtils.isNullOrEmpty(logoutSeq) 
						&& !ObjectUtils.isNullOrEmpty(logoutSeq)) {
					int logout = Integer.parseInt(NumberUtils.stringNumber(logoutSeq));
					int login = Integer.parseInt(NumberUtils.stringNumber(loginSeq));
					if(login >logout){
						return false;
					} 
					return true;
					
				} else{
					if (ObjectUtils.isNullOrEmpty(loginSeq)) {
						return false;
					}
					return true;
				}
			}
		} else if (SysDefine.LINKSTATUS.equals(msgType)){
			String linkType = dat.get(ProtocolItem.LINK_TYPE);
			if ("1".equals(linkType)
					||"2".equals(linkType)) {
				return false;
			} else if("3".equals(linkType)){
				return true;
			}
		} else if (SysDefine.REALTIME.equals(msgType)){
			return false;
		}
		return false;
	}
	
	@Override
	public List<Map<String, Object>> offlineMethod(long now){
		if (null == lastTime || lastTime.size() == 0) {
			return null;
		}
		List<Map<String, Object>>notices = new LinkedList<Map<String, Object>>();
    	String noticetime = timeformat.toDateString(new Date(now));
    	List<String> needRemoves = new LinkedList<String>();
    	for (Map.Entry<String, Long> entry : lastTime.entrySet()) {
    		long last = entry.getValue();
			if (now - last > offlinetime) {
				String vid = entry.getKey();
				needRemoves.add(vid);
				Map<String, Object> msg = vidFlyNotice.get(vid);
				if (null != msg) {
					
					getOffline(msg,noticetime);
					if (null != msg) {
						notices.add(msg);
					}
				}
				msg = vidsocNotice.get(vid);
				if (null != msg) {
					
					getOffline(msg,noticetime);
					if (null != msg) {
						notices.add(msg);
					}
				}
				msg = vidcanNotice.get(vid);
				if (null != msg) {
					
					getOffline(msg,noticetime);
					if (null != msg) {
						notices.add(msg);
					}
				}
				msg = vidIgniteShutNotice.get(vid);
				if (null != msg) {
					
					getOffline(msg,noticetime);
					if (null != msg) {
						notices.add(msg);
					}
				}
				msg = vidgpsNotice.get(vid);
				if (null != msg) {
					
					getOffline(msg,noticetime);
					if (null != msg) {
						notices.add(msg);
					}
				}
				msg = vidSpeedGtZeroNotice.get(vid);
				if (null != msg) {
					
					getOffline(msg,noticetime);
					if (null != msg) {
						notices.add(msg);
					}
				}
				msg = vidOnOffNotice.get(vid);
				if (null != msg) {
					
					getOffline(msg,noticetime);
					if (null != msg) {
						notices.add(msg);
					}
				}
			}
		}
    	
    	for (String vid : needRemoves) {
    		lastTime.remove(vid);
    		vidFlyNotice.remove(vid);
    		vidsocNotice.remove(vid);
    		vidcanNotice.remove(vid);
    		vidgpsNotice.remove(vid);
    		vidSpeedGtZeroNotice.remove(vid);
    		vidIgniteShutNotice.remove(vid);
    		vidOnOffNotice.remove(vid);
    		
    		vidFlyEd.remove(vid);
    		vidFlySt.remove(vid);
    		vidShut.remove(vid);
    		vidIgnite.remove(vid);
    		vidSpeedGtZero.remove(vid);
    		vidSpeedZero.remove(vid);
    		vidlowsoc.remove(vid);
    		vidnormsoc.remove(vid);
    		vidnocan.remove(vid);
    		vidnormcan.remove(vid);
    		vidnogps.remove(vid);
    		vidnormgps.remove(vid);
    		
		}
    	if (notices.size()>0) {
			return notices;
		}
    	
		
		return null;
	}
	
	public void getOffline(Map<String, Object> msg,String noticetime){
		if (null != msg) {
			msg.put("noticetime", noticetime);
			msg.put("status", 3);
			msg.put("etime", noticetime);
		}
	}
	
	void restartInit(boolean isRestart){
		if (isRestart) {
			recorder.rebootInit(db, onOffRedisKeys, vidOnOffNotice);
		}
	}
	public static void main(String[] args) {
		/*Map<Double, String> carSortMap = new TreeMap<Double, String>();
		carSortMap.put(256.3, "tttttt");
		carSortMap.put(122.3, "tttttt");
		carSortMap.put(356.3, "tttttt");
		carSortMap.put(299.3, "tttttt");
		carSortMap.put(156.3, "tttttt");
		carSortMap.put(756.3, "tttttt");
		
		for (Map.Entry<Double, String> entry : carSortMap.entrySet()) {
			
			System.out.println(entry.getKey()+":"+entry.getValue());
		}*/
		String la = null;
		String lt = null;
		System.out.println(la+","+lt);
	}

}
