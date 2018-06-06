package storm.handler.cusmade;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.fastjson.JSON;
import com.sun.jersey.core.util.Base64;

import org.jetbrains.annotations.NotNull;
import storm.cache.SysRealDataCache;
import storm.dao.DataToRedis;
import storm.dto.FillChargeCar;
import storm.handler.ctx.Recorder;
import storm.handler.ctx.RedisRecorder;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.protocol.SUBMIT_REALTIME;
import storm.service.TimeFormatService;
import storm.system.DataKey;
import storm.system.ProtocolItem;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.GpsUtil;
import storm.util.NumberUtils;
import storm.util.ObjectUtils;
import storm.util.ParamsRedisUtil;
import storm.util.UUIDUtils;
import storm.dto.IsSendNoticeCache;
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
	
	private Map<String, Map<String, Object>> vidLastDat;//vid和最后一帧数据的缓存
	private Map<String, Long> lastTime;
	private Map<String, IsSendNoticeCache> vidIsSendNoticeCache;
	
	DataToRedis redis;
	private Recorder recorder;
	static String onOffRedisKeys;
	
	static TimeFormatService timeformat;
	static int topn;
	static long offlinetime = 600000;//600秒
	static int socAlarm = 10;//低于10%
	static int nogpsJudgeNum = 5;//5次 
	static int hasgpsJudgeNum = 10;//10次 
	static int nocanJudgeNum = 5;//5次 
	static int hascanJudgeNum = 10;//10次 
	static int mileHop = 20;//2公里 ，单位是0.1km
	static Long nogpsIntervalTime = (long) 10800000;//10800秒
	static Long nocanIntervalTime = (long) 10800000;//10800秒
	
	
	static int db =6;
	static int socRule = 0;//1代表规则启用
	static int enableCanRule = 0;//1代表规则启用
	static int igniteRule = 0;//1代表规则启用
	static int gpsRule = 0;//1代表规则启用
	static int abnormalRule = 0;//1代表规则启用
	static int flyRule = 0;//1代表规则启用
	static int onoffRule = 0;//1代表规则启用
	static int mileHopRule = 0;//1代表规则启用
	
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
				enableCanRule = Integer.parseInt(value);
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
			
			value = ConfigUtils.sysDefine.getProperty("sys.milehop.rule");
			if (!ObjectUtils.isNullOrEmpty(value)) {
				mileHopRule = Integer.parseInt(value);
				value = null;
			}
			
		}
		init();
	}
	//以下参数可以定时进行重新加载
	static void init(){
		Object socVal = ParamsRedisUtil.PARAMS.get("lt.alarm.soc");
		if (null != socVal) {
			socAlarm = (int)socVal;
		}
		Object nocanJugyObj = ParamsRedisUtil.PARAMS.get("can.novalue.continue.no");
		if (null != nocanJugyObj) {
			nocanJudgeNum = (int)nocanJugyObj;
		}
		Object hascanJugyObj = ParamsRedisUtil.PARAMS.get("can.novalue.continue.no");
		if (null != hascanJugyObj) {
			hascanJudgeNum = (int)hascanJugyObj;
		}
		
		Object nogpsNum = ParamsRedisUtil.PARAMS.get("gps.novalue.continue.no");
		if (null != nogpsNum) {
			nogpsJudgeNum = (int)nogpsNum;
		}
		Object hasgpsNum = ParamsRedisUtil.PARAMS.get("gps.novalue.continue.no");
		if (null != hasgpsNum) {
			hasgpsJudgeNum = (int)hasgpsNum;
		}
		Object hopnum = ParamsRedisUtil.PARAMS.get("mile.hop.num");
		if (null != hopnum) {
			mileHop = ((int)hopnum)*10;
		}
		Object nogpsJudgeTime = ParamsRedisUtil.PARAMS.get("gps.judge.time");
		if (null != nogpsJudgeTime) {
			nogpsIntervalTime = ((int)nogpsJudgeTime)*1000L;
		}
		Object nocanJudgeTime = ParamsRedisUtil.PARAMS.get("can.judge.time");
		if (null != nocanJudgeTime) {
			nocanIntervalTime = ((int)nocanJudgeTime)*1000L;
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
		vidLastDat = new HashMap<String, Map<String, Object>>();
		lastTime = new HashMap<String, Long>();
		vidIsSendNoticeCache = new HashMap<String,IsSendNoticeCache>();
		
		redis = new DataToRedis();
		recorder = new RedisRecorder(redis);
		restartInit(true);
	}
	
	public static void rebulid(){
		ParamsRedisUtil.rebulid();
		init();
	}
	
	@Override
	public Map<String, Object> genotice(Map<String, String> dat) {
		
		return null;
	}

	/**
	 * 生成通知
	 * @param data
	 * @return
	 */
	@Override
    @NotNull
	public List<Map<String, Object>> generateNotices(@NotNull Map<String, String> data) {
        // 为下面的方法做准备，生成相应的容器。
        final List<Map<String, Object>> list = new LinkedList<>();

		// 验证data的有效性
		if (ObjectUtils.isNullOrEmpty(data)
				|| !data.containsKey(DataKey.VEHICLE_ID)
				|| !data.containsKey(DataKey.TIME)) {
			return list;
		}

        String vid = data.get(DataKey.VEHICLE_ID);
        if (ObjectUtils.isNullOrEmpty(vid)
				|| ObjectUtils.isNullOrEmpty(data.get(DataKey.TIME))) {
			return list;
		}
		
		lastTime.put(vid, System.currentTimeMillis());
		
		List<Map<String, Object>> socjudges = null;
		Map<String, Object> canjudge = null;
		Map<String, Object> ignite = null;
		Map<String, Object> gpsjudge = null;
		Map<String, Object> abnormaljudge = null;
		Map<String, Object> flyjudge = null;
		Map<String, Object> onoff = null;
		Map<String, Object> mileHopjudge = null;
		//3、如果规则启用了，则把dat放到相应的处理方法中。将返回结果放到list中，返回。
		if (1 == socRule){
			//lowsoc(data)返回一个map，里面有vid和通知消息（treeMap）
			socjudges = lowsoc(data);
		}
		if (1 == enableCanRule){
			canjudge = nocan(data);
		}
		if (1 == igniteRule){
			ignite = igniteShut(data);
		}
		if (1 == gpsRule){
			gpsjudge = nogps(data);
		}
		if (1 == abnormalRule){
			abnormaljudge = abnormalCar(data);
		}
		if (1 == flyRule){
			flyjudge = flySe(data);
		}
		if (1 == onoffRule){
			onoff = onOffline(data);
		}
		if (1 == mileHopRule){
			mileHopjudge = mileHopHandle(data);
		}
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
		if (! ObjectUtils.isNullOrEmpty(mileHopjudge)) {
			list.add(mileHopjudge);
		}

		return list;
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
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)) {
				return null;
			}
			String soc = dat.get(SUBMIT_REALTIME.SOC);
			String latit = dat.get(SUBMIT_REALTIME.LATITUDE);
			String longi = dat.get(SUBMIT_REALTIME.LONGITUDE);
			String location = longi+","+latit;
			String noticetime = timeformat.toDateString(new Date());
			List<Map<String, Object>> noticeMsgs = new LinkedList<Map<String, Object>>();
			if (! ObjectUtils.isNullOrEmpty(soc)) {
				double socNum = Double.parseDouble(NumberUtils.stringNumber(soc));
				//想判断是一个车辆是否为低电量，不能根据一个报文就下结论，而是要连续多个报文都是报低电量才行。
				//其他的判断也都是类似的。
				if (socNum < socAlarm) {
					int cnts = 0;
					if (vidlowsoc.containsKey(vid)) {
						cnts = vidlowsoc.get(vid);
					}
					cnts++;
					vidlowsoc.put(vid, cnts);
					if (cnts >=10) {
						//当计数器大于10以后，就去检查一下低电量列表中有没有这辆车，没有的话，就构造一条信息，放到这个列表中。
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
							//如果有了，则重新插入以下信息，覆盖之前的。
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
				jsonMap.put("LONGITUDE", chargeCar.longitude);
				jsonMap.put("LATITUDE", chargeCar.latitude);
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
	 * 无can车辆_于心沼
	 */
	private Map<String, Object> nocan(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)) {
				return null;
			}
			String canList = dat.get(ProtocolItem.CAN_LIST);
			String latit = dat.get(SUBMIT_REALTIME.LATITUDE);
			String longi = dat.get(SUBMIT_REALTIME.LONGITUDE);
			String location = longi+","+latit;
			//noticetime为当前时间
			Date date= new Date();
			String noticetime = timeformat.toDateString(date);
			
			String carStatus = dat.get(SUBMIT_REALTIME.CAR_STATUS);
			String soc = dat.get(SUBMIT_REALTIME.SOC);
			
			String macList = dat.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_LIST);
			
			String hignVolt = dat.get(SUBMIT_REALTIME.SINGLE_VOLT_HIGN_VAL);
			String lowTemp = dat.get(SUBMIT_REALTIME.SINGLE_LOWTEMP_VAL);
			
			boolean hasMacCan = false;
			if (!ObjectUtils.isNullOrEmpty(macList)){

		        String[] drivingMotors = macList.split("\\|");
		        if (drivingMotors !=null && drivingMotors.length >0){

		            for (String drivingMotor : drivingMotors){
		                String value=null;
						try {
							value = new String(Base64.decode(drivingMotor),"GBK");
							String[] params = value.split(",");
							Map<String, String> motor = new TreeMap<String, String>();
							
							for (String param : params){
								String[] p = param.split(":", 2);
								if (p!=null && p.length==2){
									motor.put(p[0], p[1]);
								}
							}
							
							if (motor.size() > 0) {
								String drivingEleMacStatus = motor.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_STATUS);//"2310";//driving 驱动电机状态
			        			String drivingEleMacTempCtol = motor.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_TEMPCTOL);//"2302";//driving 驱动电机温度控制器
			        			String drivingEleMacRev = motor.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_REV);//"2303";//driving 驱动电机转速
			        			String drivingEleMacTorque = motor.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_TORQUE);//"2311";//driving 驱动电机转矩
			        			String drivingEleMacTemp = motor.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_TEMP);//"2304";//driving 驱动电机温度
			        			String drivingEleMacVolt = motor.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_VOLT);//"2305";//driving 驱动电机输入电压
			        			String drivingEleMacEle = motor.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_ELE);//"2306";//driving 驱动电机母线电流
			        			//只要有其中之一的数据就说明有电机can状态
			        			boolean nullAll = ObjectUtils.isNullOrEmpty(drivingEleMacTemp)
			        					&& ObjectUtils.isNullOrEmpty(drivingEleMacVolt)
			        					&& ObjectUtils.isNullOrEmpty(drivingEleMacStatus)
			        					&& ObjectUtils.isNullOrEmpty(drivingEleMacTempCtol)
			        					&& ObjectUtils.isNullOrEmpty(drivingEleMacRev)
			        					&& ObjectUtils.isNullOrEmpty(drivingEleMacTorque)
			        					&& ObjectUtils.isNullOrEmpty(drivingEleMacEle);
			        			
			        			if (!nullAll) {
			        				hasMacCan = true;
									break;
								}
							}
						} catch (Exception ec) {
							ec.printStackTrace();
						}
		            }
		        }
			}
			/**
			 * 之前只是根据canList的有无判断是否有can状态，此次做了如下改进。
			 * 
			 * 判断规则（车辆状态&soc）|（电机电压&电机温度）|（最高单体电压|最低温度）
			 * 其中macList中包含了电机电压和电机温度
			 * canList，因为某些厂家的报文中可能没有这个信息，所以不能把它作为判定条件只能辅助判定。
			 */
			boolean hasCan = (
					!ObjectUtils.isNullOrEmpty(carStatus)
					&& !ObjectUtils.isNullOrEmpty(soc))
					|| (hasMacCan)
					|| (!ObjectUtils.isNullOrEmpty(hignVolt)
							&&!ObjectUtils.isNullOrEmpty(lowTemp))
					|| (!ObjectUtils.isNullOrEmpty(canList));
			
			if (!hasCan) {
				//cnts为报文计数
				int cnts = 0;
				//已有此vid，将存储的cnts赋给cnts
				if (vidnocan.containsKey(vid)) {
					cnts = vidnocan.get(vid);
				}
				cnts++;
				vidnocan.put(vid, cnts);
				if (cnts >= nocanJudgeNum) {
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
//						notice.put("status", 2);
//						notice.put("location", location);
					}
					notice.put("noticetime", noticetime);
					vidcanNotice.put(vid, notice);
					
					Long nowTime = date.getTime();
					Long firstNocanTime = timeformat.stringTimeLong(notice.get("stime").toString());
					//status，1开始，2持续，3结束
					if( 1 == (int)notice.get("status") && nowTime-firstNocanTime > nocanIntervalTime){
						IsSendNoticeCache judgeIsSendNotice = vidIsSendNoticeCache.get(vid);
						if (null == judgeIsSendNotice) {
							judgeIsSendNotice = new IsSendNoticeCache(false, true);
							vidIsSendNoticeCache.put(vid, judgeIsSendNotice);
							return notice;
						}else if (!judgeIsSendNotice.canIsSend) {
							judgeIsSendNotice.canIsSend = true;
							return notice;
						}
						
					}
				}
			}else {
				if (vidnocan.containsKey(vid)){
					int cnts = 0;
					//正常发送can报文的车辆vid
					if (vidnormcan.containsKey(vid)) {
						cnts = vidnormcan.get(vid);
					}
					cnts++;
					vidnormcan.put(vid, cnts);
					
					if (cnts >=hascanJudgeNum || (cnts >= 3 && null!=dat.get(SUBMIT_LOGIN.LOGOUT_TIME))) {
						
						vidnormcan.remove(vid);
						
						//如果无can状态开始通知没有发送，则不会发送结束通知，只会把各个缓存清空
						if(!vidIsSendNoticeCache.get(vid).canIsSend){
							vidnocan.remove(vid);
							vidcanNotice.remove(vid);
							return null;
						}
						
						Map<String, Object> notice = vidcanNotice.get(vid);
						vidnocan.remove(vid);
						vidcanNotice.remove(vid);
						vidIsSendNoticeCache.get(vid).canIsSend = false;
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
	 * 里程跳变处理
	 * @param dat
	 * @return 实时里程跳变通知notice，treemap类型。
	 * 
	 */
	private Map<String, Object> mileHopHandle(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
            String msgType = dat.get(SysDefine.MESSAGETYPE);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)
					|| ObjectUtils.isNullOrEmpty(msgType)) {
				return null;
			}
			Map<String, Object> notice = null;
			if (CommandType.SUBMIT_REALTIME.equals(msgType)) {
				
				String mileage = dat.get(SUBMIT_REALTIME.TOTAL_MILEAGE);//当前总里程
				
				if (vidLastDat.containsKey(vid)) {
					//mileage如果是数字字符串则返回，不是则返回字符串“0”
					mileage=NumberUtils.stringNumber(mileage);
					if (!"0".equals(mileage)) {
						int mile = Integer.parseInt(mileage);//当前总里程
						Map<String, Object> lastMap = vidLastDat.get(vid);
						int lastMile = (int)lastMap.get(SUBMIT_REALTIME.TOTAL_MILEAGE);//上一帧的总里程
						int nowmileHop = Math.abs(mile-lastMile);//里程跳变
						
						if (nowmileHop >= mileHop) {
                            String lastTime = (String)lastMap.get(DataKey.TIME);//上一帧的时间即为跳变的开始时间
                            String vin = dat.get(DataKey.VEHICLE_NUMBER);
							notice = new TreeMap<String, Object>();
							notice.put("msgType", "HOP_MILE");//这些字段是前端方面要求的。
							notice.put("vid", vid);
							notice.put("vin", vin);
							notice.put("stime", lastTime);
							notice.put("etime", time);
							notice.put("stmile", lastMile);
							notice.put("edmile", mile);
							notice.put("hopValue", nowmileHop);
						}
					} 
				}
				//如果vidLastDat中没有缓存此vid，则把这一帧报文中的vid、time、mileage字段缓存到LastDat
				setLastDat(dat);
			}
			
			return notice;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	private void setLastDat(Map<String, String> dat){
		String mileage = dat.get(SUBMIT_REALTIME.TOTAL_MILEAGE);
		mileage = NumberUtils.stringNumber(mileage);
		if (!"0".equals(mileage)) {
			Map<String, Object> lastMap = new TreeMap<String, Object>();
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
			int mile = Integer.parseInt(mileage);
            lastMap.put(DataKey.VEHICLE_ID, vid);
            lastMap.put(DataKey.TIME, time);
			lastMap.put(SUBMIT_REALTIME.TOTAL_MILEAGE, mile);
			vidLastDat.put(vid, lastMap);
		}
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
            String vid = dat.get(DataKey.VEHICLE_ID);
            String vin = dat.get(DataKey.VEHICLE_NUMBER);
            String time = dat.get(DataKey.TIME);
			String carStatus = dat.get(SUBMIT_REALTIME.CAR_STATUS);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)
					|| ObjectUtils.isNullOrEmpty(carStatus)) {
				return null;
			}
			
			String latit = dat.get(SUBMIT_REALTIME.LATITUDE);
			String longi = dat.get(SUBMIT_REALTIME.LONGITUDE);
			String location = longi+","+latit;
			String noticetime = timeformat.toDateString(new Date());
			String speed = dat.get(SUBMIT_REALTIME.SPEED);
			String socStr = dat.get(SUBMIT_REALTIME.SOC);
			String mileageStr = dat.get(SUBMIT_REALTIME.TOTAL_MILEAGE);
			
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
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
			String speed = dat.get(SUBMIT_REALTIME.SPEED);
			String rev = dat.get(SUBMIT_REALTIME.DRIVING_ELE_MAC_REV);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)
					|| ObjectUtils.isNullOrEmpty(speed)
					|| ObjectUtils.isNullOrEmpty(rev)) {
				return null;
			}
			double spd = Double.parseDouble(NumberUtils.stringNumber(speed));
			double rv = Double.parseDouble(NumberUtils.stringNumber(rev));
			String latit = dat.get(SUBMIT_REALTIME.LATITUDE);
			String longi = dat.get(SUBMIT_REALTIME.LONGITUDE);
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
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
			String speed = dat.get(SUBMIT_REALTIME.SPEED);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)
					|| ObjectUtils.isNullOrEmpty(speed)) {
				return null;
			}
			String latit = dat.get(SUBMIT_REALTIME.LATITUDE);
			String longi = dat.get(SUBMIT_REALTIME.LONGITUDE);
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
	 * 未定位车辆_于心沼
	 */
	Map<String, Object> nogps(Map<String, String> dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		try {
            String vid = dat.get(DataKey.VEHICLE_ID);
            String time = dat.get(DataKey.TIME);
			if (ObjectUtils.isNullOrEmpty(vid)
					|| ObjectUtils.isNullOrEmpty(time)) {
				return null;
			}
			String latit = dat.get(SUBMIT_REALTIME.LATITUDE);
			String longi = dat.get(SUBMIT_REALTIME.LONGITUDE);
			//noticetime为当前时间
			Date date= new Date();
			String noticetime = timeformat.toDateString(date);
			
			boolean isValid = true;
			if (! ObjectUtils.isNullOrEmpty(latit)
					&& !ObjectUtils.isNullOrEmpty(longi)) {
				latit = NumberUtils.stringNumber(latit);
				longi = NumberUtils.stringNumber(longi);
				double latitd = Double.parseDouble(latit);
				double longid = Double.parseDouble(longi);
				if (latitd < 0 && latitd > 180000000
						|| longid < 0 && longid > 180000000) {
					isValid = false;
				}
			}
			if (! isValid
					|| ObjectUtils.isNullOrEmpty(latit)
					|| ObjectUtils.isNullOrEmpty(longi)) {
				
				int cnts = 0;
				//vidnogps缓存无gps车辆的vid和报文帧数
				if (vidnogps.containsKey(vid)) {
					cnts = vidnogps.get(vid);
				}
				cnts++;
				vidnogps.put(vid, cnts);
				if (cnts >=nogpsJudgeNum) {
					//vidgpsNotice缓存通知，第一次发通知
					Map<String, Object> notice = vidgpsNotice.get(vid);
					if (null == notice) {
						notice =  new TreeMap<String, Object>();
						notice.put("msgType", "NO_POSITION_VEH");
						notice.put("vid", vid);
						notice.put("msgId", UUIDUtils.getUUID());
						notice.put("stime", time);
						notice.put("count", cnts);
						notice.put("status", 1);//1开始，2持续，3结束
						
					}else{
						//不是第一次了
						notice.put("count", cnts);
					}
					notice.put("noticetime", noticetime);
					vidgpsNotice.put(vid, notice);
					
					Long nowTime = date.getTime();
					Long firstNogpsTime = timeformat.stringTimeLong(notice.get("stime").toString());
					//如果满足条件，将judgeIsSend中的GpsIsSend置为true，意思是已经发送了未定位通知。
					if( 1 == (int)notice.get("status") && nowTime-firstNogpsTime > nogpsIntervalTime){
						IsSendNoticeCache judgeIsSendNotice = vidIsSendNoticeCache.get(vid);
						if (null == judgeIsSendNotice) {
							judgeIsSendNotice = new IsSendNoticeCache(true, false);
							vidIsSendNoticeCache.put(vid, judgeIsSendNotice);
							return notice;
						}else if (!judgeIsSendNotice.gpsIsSend) {
							judgeIsSendNotice.gpsIsSend = true;
							return notice;
						}
						
					}
				}
			}else {
				if (vidnogps.containsKey(vid)){//车的GPS是有效的，并且vidnogps中包含这辆车，才有可能发送结束通知报文
					int cnts = 0;
					if (vidnormgps.containsKey(vid)) {
						cnts = vidnormgps.get(vid);
					}
					cnts++;
					vidnormgps.put(vid, cnts);
					//有效gps报文超过hasgpsJudgeNum  || 有效gps报文在（3贞以上，10贞以下）同时车辆登出
					if (cnts >=hasgpsJudgeNum || (cnts > 3 && null!=dat.get(SUBMIT_LOGIN.LOGOUT_TIME)) ) {
						
						vidnormgps.remove(vid);
						//如果未定位开始通知没有发送，则不会发送结束通知，只会把各个缓存清空
						if(!vidIsSendNoticeCache.get(vid).gpsIsSend){
							vidnogps.remove(vid);
							vidgpsNotice.remove(vid);
							return null;
						}
						
						Map<String, Object> notice = vidgpsNotice.get(vid);
						vidnogps.remove(vid);
						vidgpsNotice.remove(vid);
						vidIsSendNoticeCache.get(vid).gpsIsSend = false;
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
            String vid = dat.get(DataKey.VEHICLE_ID);
            String vin = dat.get(DataKey.VEHICLE_NUMBER);
            String time = dat.get(DataKey.TIME);
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
		if (CommandType.SUBMIT_LOGIN.equals(msgType)) {
			String type = dat.get(ProtocolItem.REG_TYPE);
			if ("1".equals(type)){
				return false;
			} else if ("2".equals(type)){
				return true;
			} else {
				String logoutTime = dat.get(SUBMIT_LOGIN.LOGOUT_TIME);
				String loginTime = dat.get(SUBMIT_LOGIN.LOGIN_TIME);
				if (! ObjectUtils.isNullOrEmpty(logoutTime) 
						&& !ObjectUtils.isNullOrEmpty(logoutTime)) {
					long logout = Long.parseLong(NumberUtils.stringNumber(logoutTime));
					long login = Long.parseLong(NumberUtils.stringNumber(loginTime));
					if(login >logout){
						return false;
					} 
					return true;
					
				} else{
					if (ObjectUtils.isNullOrEmpty(loginTime)) {
						return false;
					}
					return true;
				}
			}
		} else if (CommandType.SUBMIT_LINKSTATUS.equals(msgType)){
			String linkType = dat.get(SUBMIT_LINKSTATUS.LINK_TYPE);
			if ("1".equals(linkType)
					||"2".equals(linkType)) {
				return false;
			} else if("3".equals(linkType)){
				return true;
			}
		} else if (CommandType.SUBMIT_REALTIME.equals(msgType)){
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
				Map<String, Object> msg = vidOnOffNotice.get(vid);//vidOnOffNotice是车辆上线通知缓存
				//如果msg不为null，说明之前在线，现在离线，需要发送离线通知
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
    		vidOnOffNotice.remove(vid);
		}
    	if (notices.size()>0) {
			return notices;
		}
		return null;
	}
	
	public List<Map<String, Object>> offlineMethod2(long now){
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
