package storm.handler.cal;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctfo.datacenter.cache.handle.CTFOCacheKeys;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import storm.cache.util.RedisOrganizationUtil;
import storm.dao.DataToRedis;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.CTFOUtils;
import storm.util.ConfigUtils;
import storm.util.NumberUtils;

/**
 * 计算碳排里程等数据的处理类
 * @author 76304
 *
 */
public class MailCalHandler {
	private static Logger logger = LoggerFactory.getLogger(MailCalHandler.class);
	private static final ConfigUtils configUtils = ConfigUtils.getInstance();
	public static final String unknow="UNKNOW";
	public static final String unknowAndunknow="UNKNOW,UNKNOW";
	public static final String unknowString="UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW,UNKNOW";
	public static final String [] unknowArray =new String[]{"UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW","UNKNOW"};
	private static DataToRedis redis;
	private static long time = 180 * 1000 ;
	private static long stoptime = 180 * 1000 ;
	public static boolean redisclusterIsload;
	public static Cache<String, String[]>carInfoArray = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(15000000)
			.build();
	
	public static Cache<String,Set<String>> carUserCache = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(1000000)
			.build();
	
	public static Cache<String,String> districtParentCache = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(1000000)
			.build();
	public static Cache<String,Map<String,String>> carlastrecord = CacheBuilder.newBuilder()
			.expireAfterAccess(60,TimeUnit.MINUTES)
			.maximumSize(15000000)
			.build();
	private static Map<String, Map<String,String>> zeroCache = new java.util.concurrent.ConcurrentHashMap<String, Map<String,String>>();
	static{
		setTime();
		redis=new DataToRedis();
		Map<String, String> map = redis.hgetallMapByKeyAndDb("XNY.CARINFO", 0);
		Map<String,Set<String>> carUsers = RedisOrganizationUtil.getCarUser(false);
		carUserCache.putAll(carUsers);
		for (Map.Entry<String, String> entry : map.entrySet()) {
			try {
				String key = entry.getKey();
				String value = entry.getValue();

				if (StringUtils.isEmpty(key)
						|| StringUtils.isEmpty(value)) {
					continue;
				}

				String []strings=value.split(",",-1);
				
				if(strings.length != 15) {
					continue;
				}
				carInfoArray.put(key, strings);
			}catch (Exception e) {
					
			}
		}
		redisclusterIsload = loadLastrecordByRediscluster();
		tasks();
	}
	private static void resetAllCache(){
		if (null != carInfoArray) {
			carInfoArray.cleanUp();
		}
		Map<String, String> map = redis.hgetallMapByKeyAndDb("XNY.CARINFO", 0);
		for (Map.Entry<String, String> entry : map.entrySet()) {
			try {
				String key = entry.getKey();
				String value = entry.getValue();

				if (StringUtils.isEmpty(key)
						|| StringUtils.isEmpty(value)) {
					continue;
				}

				String []strings=value.split(",",-1);
				
				if(strings.length != 15) {
					continue;
				}
				carInfoArray.put(key, strings);
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		resetUserCache();
	}
	private static void setTime(){
		String offli= configUtils.sysDefine.getProperty(StormConfigKey.REDIS_OFFLINE_SECOND);
		if(null != offli) {
			time=1000*Long.valueOf(offli);
		}
		String stopli= configUtils.sysDefine.getProperty("redis.offline.stoptime");
		if(null != stopli) {
			stoptime=1000*Long.valueOf(stopli);
		}
	}
	private static void tasks(){
		long clustertime = 180L;
		long stattime = 600L;
		try {
			String cltt = configUtils.sysDefine.getProperty("redis.totalInterval");
			String stt = configUtils.sysDefine.getProperty("redis.monitor.time");
			if (null != cltt) {
				clustertime=Long.parseLong(cltt);
			}
			if (null != stt) {
				stattime=Long.parseLong(stt);
			}
			
		} catch (Exception e) {
			
		}
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new TimerThread(), 0, clustertime, TimeUnit.SECONDS);
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new StatCarinfoThread(), 0, stattime, TimeUnit.SECONDS);
	}

	private static boolean loadLastrecordByRediscluster(){
		boolean keyLoadComp=false;
		ExecutorService threadPool=Executors.newCachedThreadPool();
		CTFOCacheKeys ctfoCacheKeys=null;
        try {
            ctfoCacheKeys = CTFOUtils.getDefaultCTFOCacheTable().getCTFOCacheKeys();
            List<OneKeysLoader> executors=new LinkedList<OneKeysLoader>();
            while(ctfoCacheKeys.next()){
            	List<String>keys=ctfoCacheKeys.getKeys();
            	executors.add(new OneKeysLoader(keys));
            }
            for (OneKeysLoader executor : executors) {
				threadPool.execute(executor);
			}
            boolean exe=true;
            while(exe){
            	boolean allComplete=true;
            	for (OneKeysLoader executor : executors) {
					if(!executor.isComplete()){
						allComplete=false;
						TimeUnit.MILLISECONDS.sleep(100);
						break;
					}
				}
            	if (allComplete) {
					exe=false;
				}
            }
            keyLoadComp = true;
            ctfoCacheKeys=null;
            executors=null;
        } catch (Exception e) {
            System.out.println("--------redis集群初始化实时数据计算异常！" + e);
            e.printStackTrace();
        }
        return keyLoadComp;
	}
	public static void addCarCache(Map<String, String> map){
		Map<String, String> newmap =  new TreeMap<String, String>();
		//不缓存无用的数据项，减小缓存大小
		for (Map.Entry<String, String> entry : map.entrySet()) {
			String mapkey=entry.getKey();
			String value=entry.getValue();
			if (null!= mapkey && null !=value 
					&& !mapkey.startsWith("useful")
					&& !mapkey.startsWith("newest")
					&& !"2001".equals(mapkey)
					&& !"2002".equals(mapkey)
					&& !"2003".equals(mapkey)
					&& !"2101".equals(mapkey)
					&& !"2103".equals(mapkey)
					&& !"7001".equals(mapkey)
					&& !"7003".equals(mapkey)
					&& !"7101".equals(mapkey)
					&& !"7103".equals(mapkey)) {
				newmap.put(mapkey, value);
			}
		}
		String vid = newmap.get(DataKey.VEHICLE_ID);
		carlastrecord.put(vid, newmap);
	}
	
	static class OneKeysLoader implements Runnable,Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 112345600014L;
		private List<String>keys;
		private boolean complete;
		public OneKeysLoader(List<String> keys) {
			super();
			this.keys = keys;
			complete = false;
		}

		@Override
		public void run() {

			if(null!=keys && keys.size()>0) {
				loadBykeys(keys);
			}
			complete = true;
		}
		
		/**
		 * 从redis 集群中获取数据用于统计
		 * @param keys
		 */
		void loadBykeys(List<String> keys){
			try {
				for(String key :keys){
					if (StringUtils.isEmpty(key)) {
						continue;
					}
					key=key.split("-",3)[2];
					if (StringUtils.isEmpty(key)) {
						continue;
					}
					Map<String, String> map=CTFOUtils.getDefaultCTFOCacheTable().queryHash(key);
                    if(MapUtils.isEmpty(map)) {
						continue;
					}
					carlastrecord.put(key, map);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public boolean isComplete() {
			return complete;
		}
	}
		
	public static String[] carInfoByVin(final String vin){
		String []carArr=unknowArray;
		try {
			carArr=carInfoArray.get(vin, new Callable<String[]>() {

				@Override
				public String[] call() throws Exception {
					
					return unknowArray;
				}
			});
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return carArr;
	}

	private static void resetUserCache(){
		try {
			boolean bool = false;
			Set<String>sets=redis.getKeysSet(10, "XNY.INFOCHANG.*");
			if(null!=sets && 0<sets.size()){

				for (String str : sets) {
					bool="1".equals(redis.getString(10, str));
					if (bool) {
						break;
					}
				}
				if (bool) {
					for (String str : sets) {
						redis.setString(10, str, "0");
					}
					carUserCache.cleanUp();
					Map<String,Set<String>> carUsers = RedisOrganizationUtil.getCarUser(bool);
					carUserCache.putAll(carUsers);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Set<String> carUserByVin(final String vin){
		Set<String> users=null;
		
		final boolean change=false;
		try {
			users=carUserCache.get(vin, new Callable<Set<String>>() {

				@Override
				public Set<String> call() throws Exception {
					Map<String, Set<String>> map=RedisOrganizationUtil.getCarUser(change);
					if(null==map) {
						return null;
					}
					Set<String> set=map.get(vin);
					return set;
				}
			});
		} catch (Exception e) {
//			e.printStackTrace();
		}
		return users;
	}

	public static Set<String> parentDistrictByVin(String districtId){
		try {
			return RedisOrganizationUtil.getDistrictClassify().get(districtId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void init(){
		RedisOrganizationUtil.init();
	}

	static class TimerThread implements Runnable{
		private Map<String, Map<String,String>>infoMap;
		private Map<String, Map<String,String>>infoNoDistMap;
		private Map<String, Map<String,String>>infoTotalMap;
		private Map<String, Map<String,String>>infoTotalDisMap;
		private Map<String, Map<String,String>>districtTotalInfo;
		private Map<String, Map<String,String>>districtTotalTypeInfo;
		private Map<String, Map<String,String>>orgDistAndTypeMap;
		private Map<String, Map<String,String>>orgOnlyDistMap;
		private Map<String, Map<String,String>>orgOnlyCarTypeMap;
		private Map<String, Map<String,String>>plantOnlyCarTypeMap;
		
		private Map<String, Map<String, Map<String,String>>>userCacheDistAndTypeMap;
		private Map<String, Map<String, Map<String,String>>>userCacheOnlyDistMap;
		private Map<String, Map<String, Map<String,String>>>userCacheOnlyCarTypeMap;
		private Map<String, Map<String, Map<String,String>>>userTotalDistAndTypeMap;
		private Map<String, Map<String, Map<String,String>>>userTotalOnlyDistMap;
		private static int poolsz = 30;
		private ExecutorService threadPools;
		private Map<String, Boolean>vinMap;
		private boolean complete;
		private static boolean isclear=false;
		private static long stime =0L;
		{
			infoMap=new HashMap<String,Map<String, String>>();
			infoNoDistMap=new HashMap<String,Map<String, String>>();
			infoTotalMap=new HashMap<String,Map<String, String>>();
			infoTotalDisMap=new HashMap<String,Map<String, String>>();
			districtTotalInfo=new HashMap<String,Map<String, String>>();
			districtTotalTypeInfo=new HashMap<String,Map<String, String>>();
			
			orgDistAndTypeMap=new HashMap<String,Map<String, String>>();
			orgOnlyDistMap=new HashMap<String,Map<String, String>>();
			orgOnlyCarTypeMap=new HashMap<String,Map<String, String>>();
			plantOnlyCarTypeMap=new HashMap<String,Map<String, String>>();
			
			userCacheDistAndTypeMap=new HashMap<String, Map<String, Map<String,String>>>();
			userCacheOnlyDistMap=new HashMap<String, Map<String, Map<String,String>>>();
			userCacheOnlyCarTypeMap=new HashMap<String, Map<String, Map<String,String>>>();
			userTotalDistAndTypeMap=new HashMap<String, Map<String, Map<String,String>>>();
			userTotalOnlyDistMap=new HashMap<String, Map<String, Map<String,String>>>();
			
			vinMap=new HashMap<String,Boolean>();
			setComplete(false);
			
			stime = 1506790800000L;//2017/10/01/01:00:00的long值时间
			String threads= configUtils.sysDefine.getProperty("stat.thread.no");
			if(null != threads) {
				poolsz=Integer.valueOf(threads);
			}
		}
		void setComplete(boolean complete) {
			this.complete = complete;
		}
		
		public static void setIsclear(boolean isclear) {
			TimerThread.isclear = isclear;
		}

		@Override
        public void run() {
            try {
            	int db=9;
            	long now = System.currentTimeMillis();
            	if ((now -stime)%86400000<=180000) {
            		redis.flushDB(db);
				}
            	
            	if (!isclear) {
					
            		setComplete(false);
        			if(null!=carlastrecord.asMap() && carlastrecord.asMap().size()>0) {
						executeByMaps(carlastrecord.asMap().values());
					}
            		if (complete) {
            			saveToRedis(db);
            			setComplete(false);
            			setIsclear(false);
					}
				}
            	
            } catch (Exception e) {
                System.out.println("------临时统计任务异常！" + e);
            }
        }

		private void saveToRedis(int db){
			try {
				saveMap(infoMap,db);
				saveMap(infoNoDistMap,db);
				saveMap(infoTotalMap,db);
				saveMap(infoTotalDisMap,db);
				saveMap(districtTotalInfo,db);
				saveMap(districtTotalTypeInfo,db);
				
				Collection<Map<String, Map<String, String>>> collection=null;
				collection=userCacheDistAndTypeMap.values();
				saveCollection(collection,db);
				
				collection=userCacheOnlyDistMap.values();
				saveCollection(collection,db);
				
				collection=userCacheOnlyCarTypeMap.values();
				saveCollection(collection,db);
				
				collection=userTotalDistAndTypeMap.values();
				saveCollection(collection,db);
				
				collection=userTotalOnlyDistMap.values();
				saveCollection(collection,db);
				collection=null;
				
				saveMap(orgDistAndTypeMap,db);
				saveMap(orgOnlyDistMap,db);
				saveMap(orgOnlyCarTypeMap,db);
				saveMap(plantOnlyCarTypeMap,db);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		private void saveCollection(Collection<Map<String, Map<String, String>>> collection,int db){
			try {
				for (Map<String, Map<String, String>> map : collection) {
					saveMap(map,db);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private void saveMap(Map<String, Map<String, String>> map,int db){
			try {
				if(null!=map) {
					for(Map.Entry<String, Map<String,String>> entry:map.entrySet()){
						String key = entry.getKey();
						Map<String,String> value = entry.getValue();
                        if (StringUtils.isEmpty(key)
								|| MapUtils.isEmpty(value)) {
							continue;
						}

						redis.saveMap(value, db, key);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		void executeByMaps(Collection<Map<String, String>> maps){
			try {
				if (null == maps || maps.size()<1) {
					return;
				}
				clearMap();
				threadPools = Executors.newFixedThreadPool(poolsz);
				for(Map<String, String> map :maps){
                    if (MapUtils.isEmpty(map)) {
						continue;
					}
					threadPools.execute(new UnitDatCal(map));
				}
				try {
					threadPools.shutdown();
					while(true){
						if (threadPools.isTerminated()) {
							break;
						}
						TimeUnit.MILLISECONDS.sleep(500);
					}
					threadPools=null;
					complete = true;
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		class UnitDatCal implements Runnable{

			private Map<String, String> dat;
			
			public UnitDatCal(Map<String, String> dat) {
				super();
				this.dat = dat;
			}

			@Override
			public void run() {
				if (null != dat) {
					statistic(dat);
				}
					
			}
			void statistic(Map<String, String> map){
				try {
                    if(!MapUtils.isEmpty(map)){
						
						String vin=map.get("VIN");
						if(!vinMap.containsKey(vin)){
							
							vinMap.put(vin, true);
							if (!StringUtils.isEmpty(vin)) {
								String []strings=carInfoByVin(vin);
								if(strings.length != 15){
									logger.info("vin carInfo:"+vin);
									strings=unknowArray;
								}
								if (null != strings[0] && strings[0].equals(map.get(DataKey.VEHICLE_ID))){
									
									String org=strings[3];
									String plant=strings[5];
									String district=strings[9];
									String carType=strings[10];
									String keyDistAndType="CARINFO."+district+"."+carType;
									execu(map,keyDistAndType,infoMap);
									String keyOnlyCarType="CARINFO.TOTAL."+district;
									execu(map,keyOnlyCarType,infoTotalMap);
									String keyOnlyDist="CARINFO.DISTOTAL."+carType;
									execu(map,keyOnlyDist,infoTotalDisMap);
									
									Set<String> keyDistricts=parentDistrictByVin(district);
									if(null!=keyDistricts && 0<keyDistricts.size()){
										for (String dis : keyDistricts) {
											String keyDist="CARINFO.DISTRICTS." + dis;
											execu(map,keyDist,districtTotalInfo);
											
											String keyDistType="CARINFO.DISTRICTS." + dis + "." + carType;
											execu(map,keyDistType,districtTotalTypeInfo);
										}
									}
									
									if(unknow.equals(district)){
										String keyNoUnk="CARINFO.NOUNKNOW."+carType;
										execu(map,keyNoUnk,infoNoDistMap);
									}

									org= StringUtils.isEmpty(org) ?unknow:org;
									String orgDistAndType="ORGCAR."+org+"."+district+"."+carType;
									execu(map,orgDistAndType,orgDistAndTypeMap);
									
									String orgOnlyDist="ORGCAR."+org+".DIST."+district;
									execu(map,orgOnlyDist,orgOnlyDistMap);
									
									String orgOnlyCarType="ORGCAR."+org+".CARTYPE."+carType;
									execu(map,orgOnlyCarType,orgOnlyCarTypeMap);
									
									String plantOnlyCarType="PLANTCAR."+plant+".CARTYPE."+carType;
									execu(map,plantOnlyCarType,plantOnlyCarTypeMap);
									
									Set<String>users=carUserByVin(vin);
									if(null!=users) {
										for (String user : users) {
											if (StringUtils.isEmpty(user)) {
												continue;
											}
											Map<String, Map<String,String>>userDistAndTypeMap=userCacheDistAndTypeMap.get(user);
											if (null==userDistAndTypeMap) {
												userDistAndTypeMap = new HashMap<String, Map<String, String>>();
											}

											Map<String, Map<String,String>>userOnlyDistMap=userCacheOnlyDistMap.get(user);
											if (null==userOnlyDistMap) {
												userOnlyDistMap = new HashMap<String, Map<String, String>>();
											}

											Map<String, Map<String,String>>userOnlyCarTypeMap=userCacheOnlyCarTypeMap.get(user);
											if (null==userOnlyCarTypeMap) {
												userOnlyCarTypeMap = new HashMap<String, Map<String, String>>();
											}

											String userDistAndType="USERCAR."+user+"."+district+"."+carType;
											execu(map,userDistAndType,userDistAndTypeMap);

											String userOnlyDist="USERCAR."+user+".DIST."+district;
											execu(map,userOnlyDist,userOnlyDistMap);

											String userOnlyCarType="USERCAR."+user+".CARTYPE."+carType;
											execu(map,userOnlyCarType,userOnlyCarTypeMap);

											userCacheDistAndTypeMap.put(user, userDistAndTypeMap);
											userCacheOnlyDistMap.put(user, userOnlyDistMap);
											userCacheOnlyCarTypeMap.put(user, userOnlyCarTypeMap);

											if(null!=keyDistricts && 0<keyDistricts.size()){

												Map<String, Map<String,String>>userDistrictsAndTypeMap=userTotalDistAndTypeMap.get(user);
												if (null==userDistrictsAndTypeMap) {
													userDistrictsAndTypeMap = new HashMap<String, Map<String, String>>();
												}

												Map<String, Map<String,String>>userOnlyDistrictsMap=userTotalOnlyDistMap.get(user);
												if (null==userOnlyDistrictsMap) {
													userOnlyDistrictsMap = new HashMap<String, Map<String, String>>();
												}

												for (String dis : keyDistricts) {
													String keyDist="USERCAR.DISTRICTS."+user+"."+dis;
													execu(map,keyDist,userDistrictsAndTypeMap);

													String keyDistType="USERCAR.DISTRICTS."+user+"."+dis+"."+carType;
													execu(map,keyDistType,userOnlyDistrictsMap);
												}
												userTotalDistAndTypeMap.put(user, userDistrictsAndTypeMap);
												userTotalOnlyDistMap.put(user, userOnlyDistrictsMap);
											}
										}
									}
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
		
		synchronized void execu(Map<String, String> map, String keyty, Map<String, Map<String,String>> infoMap){
        	try {
				Map<String, String> disMap = infoMap.get(keyty);
				if(null==disMap) {
					disMap=new TreeMap<String, String>();
				}
				long mileage=null==disMap.get(SysDefine.MILEAGE_TOTAL)?0:Long.valueOf(disMap.get(SysDefine.MILEAGE_TOTAL));
				long online=null==disMap.get(SysDefine.ONLINE_COUNT)?0:Long.valueOf(disMap.get(SysDefine.ONLINE_COUNT));
				long fault=null==disMap.get(SysDefine.FAULT_COUNT)?0:Long.valueOf(disMap.get(SysDefine.FAULT_COUNT));
				long cartotal=null==disMap.get(SysDefine.CAR_COUNT)?0:Long.valueOf(disMap.get(SysDefine.CAR_COUNT));
				long charge=null==disMap.get(SysDefine.CHARGE_CAR_COUNT)?0:Long.valueOf(disMap.get(SysDefine.CHARGE_CAR_COUNT));
				long runningonline=null==disMap.get(SysDefine.RUNNING_ONLINE)?0:Long.valueOf(disMap.get(SysDefine.RUNNING_ONLINE));
				long stoponline=null==disMap.get(SysDefine.STOP_ONLINE)?0:Long.valueOf(disMap.get(SysDefine.STOP_ONLINE));
				
				mileage +=Long.parseLong(NumberUtils.stringNumber(map.get(DataKey._2202_TOTAL_MILEAGE)));
				long lastTime=Long.valueOf(map.get(SysDefine.ONLINEUTC));
				if (System.currentTimeMillis()-lastTime<time){
					online++;
					boolean isstop=isStop(map);
					if (isstop) {
						stoponline++;
					} else {
						runningonline++;
					}
				}
				if("1".equals(map.get(SysDefine.ISALARM)) && null != map.get(SysDefine.ALARMUTC)) {
					fault++;
				}
				if("1".equals(map.get("2301")) 
						|| "2".equals(map.get("2301"))
						|| "4".equals(map.get("2301"))) {
					charge++;
				}
				cartotal++;
				disMap.put(SysDefine.MILEAGE_TOTAL,""+mileage);
				disMap.put(SysDefine.ONLINE_COUNT,""+online);
				disMap.put(SysDefine.FAULT_COUNT,""+fault);
				disMap.put(SysDefine.CAR_COUNT,""+cartotal);
				disMap.put(SysDefine.CHARGE_CAR_COUNT,""+charge);
				disMap.put(SysDefine.RUNNING_ONLINE,""+runningonline);
				disMap.put(SysDefine.STOP_ONLINE,""+stoponline);
				infoMap.put(keyty, disMap);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }

		boolean isStop(Map<String, String> map){
			try {
				String vid = map.get(DataKey.VEHICLE_ID);
				String rev = map.get("2303");
				String spd = map.get("2201");
				if (!"0".equals(spd) || !"20000".equals(rev)) {
					zeroCache.remove(vid);
					return false;
				}
				if ("0".equals(spd) && "20000".equals(rev)){
					String timelong = map.get(SysDefine.ONLINEUTC);
					String lon = map.get("2502");//经度
					String lan = map.get("2503");//纬度
					
					Map<String , String>startZero=zeroCache.get(vid);
					if (null == startZero) {
						startZero = new TreeMap<String , String>();
						startZero.put("2303", rev);
						startZero.put("2201", spd);
						startZero.put("2502", lon);
						startZero.put("2503", lan);
						startZero.put(SysDefine.ONLINEUTC, timelong);
						
						zeroCache.put(vid, startZero);
						return false;
					} else {
						long lastTime=Long.valueOf(map.get(SysDefine.ONLINEUTC));
						long starttime=Long.valueOf(startZero.get(SysDefine.ONLINEUTC));
						if (lastTime - starttime >= stoptime) {
							String slon = startZero.get("2502");//经度
							String slan = startZero.get("2503");//纬度
							if ( ( StringUtils.isEmpty(lon)
										|| StringUtils.isEmpty(lan))
									&& ( StringUtils.isEmpty(slon)
											|| StringUtils.isEmpty(slan)) ) {
								return true;
							}
							if (   !StringUtils.isEmpty(lon)
								&& !StringUtils.isEmpty(lan)
								&& !StringUtils.isEmpty(slon)
								&& !StringUtils.isEmpty(slan)){
								long longi = Long.valueOf(lon);
								long slongi = Long.valueOf(slon);
								long lati = Long.valueOf(lan);
								long slati = Long.valueOf(slan);
								if (Math.abs(longi-slongi)<=2
										&& Math.abs(lati-slati)<=2) {
									return true;
								}
							}
								
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return false;
		}
		
        private void clearMap(){
        	if(infoMap.size()>0){
        		infoMap.clear();
        		infoNoDistMap.clear();
        		infoTotalMap.clear();
        		infoTotalDisMap.clear();
        		districtTotalInfo.clear();
        		districtTotalTypeInfo.clear();
        		orgDistAndTypeMap.clear();
    			orgOnlyDistMap.clear();
    			orgOnlyCarTypeMap.clear();
    			plantOnlyCarTypeMap.clear();
    			
    			Collection<Map<String, Map<String, String>>> collection=null;
    			
    			collection=userCacheDistAndTypeMap.values();
    			clearCollection(collection);
    			
    			collection=userCacheOnlyDistMap.values();
    			clearCollection(collection);
    			
    			collection=userCacheOnlyCarTypeMap.values();
    			clearCollection(collection);
    			
    			collection=userTotalDistAndTypeMap.values();
    			clearCollection(collection);
    			
    			collection=userTotalOnlyDistMap.values();
    			clearCollection(collection);
    			
    			collection=null;
    			isclear=true;
        	}
        }
        void clearCollection(Collection<Map<String, Map<String, String>>> collection){
        	for (Map<String, Map<String, String>> map : collection) {
				if(null!=map) {
					map.clear();
				}
			}
        }
    }

	static class StatCarinfoThread implements Runnable{

		private Map<String, Map<String, String>> stadistrictsTotalMap;
		private Map<String, Map<String, String>> stadistrictsTotalTypeMap;
		private Map<String, Map<String, String>> staplantOnlyCarTypeMap;
		private Map<String,Map<String, Map<String, String>>> stauserTotalDistAndTypeMap;
		private Map<String,Map<String, Map<String, String>>> stauserTotalOnlyDistMap;
		private static boolean staComp =false;
		private static boolean isclearSta =false;
		private static int poolsz = 50;
		private ExecutorService threadPools;
		{
			stadistrictsTotalMap = new ConcurrentHashMap<String, Map<String, String>>();
			stadistrictsTotalTypeMap = new ConcurrentHashMap<String, Map<String, String>>();
			staplantOnlyCarTypeMap = new ConcurrentHashMap<String, Map<String, String>>();
			stauserTotalDistAndTypeMap = new ConcurrentHashMap<String,Map<String, Map<String, String>>>();
			stauserTotalOnlyDistMap = new ConcurrentHashMap<String,Map<String, Map<String, String>>>();
			String threads= configUtils.sysDefine.getProperty("stat.thread.no");
			if(null != threads) {
				poolsz=Integer.valueOf(threads);
			}
			threadPools = Executors.newFixedThreadPool(poolsz);
		}

        
		@Override
		public void run() {
			if (!isclearSta) {
        		statistic(carInfoArray.asMap());
        		if (staComp) {
        			saveOrUpdate(9);
        			staComp=false;
        			resetAllCache();
				}
        		isclearSta=false;
			}

		}

/*******************************************************/
        
		void saveOrUpdate(int db){
    		saveMap(stadistrictsTotalMap,db);
    		saveMap(stadistrictsTotalTypeMap,db);
    		
    		Collection<Map<String, Map<String, String>>> collection=null;
    		collection=stauserTotalDistAndTypeMap.values();
    		saveCollection(collection,db);
    		
    		collection=stauserTotalOnlyDistMap.values();
    		saveCollection(collection,db);
    		collection=null;
    		
		}
		
		void statistic(Map<String, String[]> map){
			if (isclearSta) {
				return;
			}
			if (null == map || map.size() ==0) {
				return;
			}

			System.out.println("---map size:"+map.size());
			clearStaMap();
			staComp=false;
			for (Map.Entry<String, String[]> entry : map.entrySet()) {
				try {
					String key = entry.getKey();
					String[] strings = entry.getValue();

                    if (StringUtils.isEmpty(key)
							|| ArrayUtils.isEmpty(strings)) {
						continue;
					}

					if(strings.length != 15) {
						continue;
					}

					threadPools.execute(new UserDistrictExecutor(key,strings));
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try {
				threadPools.shutdown();
				while(true){
					if (threadPools.isTerminated()) {
						break;
					}
					TimeUnit.MILLISECONDS.sleep(500);
				}
				threadPools=null;
				threadPools = Executors.newFixedThreadPool(poolsz); 
				staComp = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		class UserDistrictExecutor implements Runnable{
			private String key;
			private String[]strings;
			
			public UserDistrictExecutor(String key, String[] strings) {
				super();
				this.key = key;
				this.strings = strings;
			}

			@Override
			public void run() {
				executeCal(key,strings);
			}
			
			void executeCal(String key,String[]strings){
				if (null == key || null == strings) {
					return;
				}
				try {
					String district=strings[9];
					String carType=strings[10];
					String monitor=strings[14];
					boolean ismonitor="1".equals(monitor);
					String plant=strings[5];
					if (!StringUtils.isEmpty(plant) && !StringUtils.isEmpty(carType)) {
						
						String plantOnlyCarType="STATISTIC.PLANTCAR."+plant+".CARTYPE."+carType;
						exeCalculate(plantOnlyCarType,staplantOnlyCarTypeMap,ismonitor);
					}

					if (!StringUtils.isEmpty(district)){
						
						Set<String> districts=parentDistrictByVin(district);
						
						if (null != districts && districts.size()>0) {
							for (String dis : districts) {
								String keyDist="STATISTIC.DISTRICTS."+dis;
								exeCalculate(keyDist,stadistrictsTotalMap,ismonitor);
								
								String keyDistType="STATISTIC.DISTRICTS."+dis+"."+carType;
								exeCalculate(keyDistType,stadistrictsTotalTypeMap,ismonitor);
							}
						}
						
						Set<String>users=carUserByVin(key);
						if(null!=users) {
							for (String user : users) {
								if (StringUtils.isEmpty(user)) {
									continue;
								}

								if(null!=districts && 0<districts.size()){

									Map<String, Map<String,String>>userDistrictsAndTypeMap=stauserTotalDistAndTypeMap.get(user);
									if (null==userDistrictsAndTypeMap) {
										userDistrictsAndTypeMap = new HashMap<String, Map<String, String>>();
									}

									Map<String, Map<String,String>>userOnlyDistrictsMap=stauserTotalOnlyDistMap.get(user);
									if (null==userOnlyDistrictsMap) {
										userOnlyDistrictsMap = new HashMap<String, Map<String, String>>();
									}

									for (String dis : districts) {
										String keyDist="STATISTIC.USER.DISTRICTS."+user+"."+dis;
										exeCalculate(keyDist,userDistrictsAndTypeMap,ismonitor);

										String keyDistType="STATISTIC.USER.DISTRICTS."+user+"."+dis+"."+carType;
										exeCalculate(keyDistType,userOnlyDistrictsMap,ismonitor);
									}
									stauserTotalDistAndTypeMap.put(user, userDistrictsAndTypeMap);
									stauserTotalOnlyDistMap.put(user, userOnlyDistrictsMap);
								}
							}
						}
					} 
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		synchronized void exeCalculate(String key,Map<String, Map<String,String>>infoMap,boolean ismonitor){
	    	try {
				Map<String, String>disMap=infoMap.get(key);
				if(null==disMap) {
					disMap=new TreeMap<String, String>();
				}
				long monitorcount=null==disMap.get(SysDefine.MONITOR_CAR_TOTAL)?0:Long.valueOf(disMap.get(SysDefine.MONITOR_CAR_TOTAL));
				long cartotal=null==disMap.get(SysDefine.CAR_TOTAL)?0:Long.valueOf(disMap.get(SysDefine.CAR_TOTAL));

				cartotal++;
				if (ismonitor) {
					monitorcount++;
				}

				disMap.put(SysDefine.MONITOR_CAR_TOTAL,""+monitorcount);
				disMap.put(SysDefine.CAR_TOTAL,""+cartotal);
				infoMap.put(key, disMap);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }

		private void saveCollection(Collection<Map<String, Map<String, String>>> collection,int db){
			try {
				for (Map<String, Map<String, String>> map : collection) {
					saveMap(map,db);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private void saveMap(Map<String, Map<String, String>> map,int db){
			try {
				if(null!=map) {
					for(Map.Entry<String, Map<String,String>> entry:map.entrySet()){
						String key = entry.getKey();
						Map<String,String> value = entry.getValue();
                        if (StringUtils.isEmpty(key)
								|| MapUtils.isEmpty(value)) {
							continue;
						}

						redis.saveMap(value, db, key);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		void clearStaMap(){
			stadistrictsTotalMap.clear();
    		stadistrictsTotalTypeMap.clear();
    		staplantOnlyCarTypeMap.clear();
			
			Collection<Map<String, Map<String, String>>> collection=null;
			
			collection=stauserTotalDistAndTypeMap.values();
			clearCollection(collection);
			
			collection=stauserTotalOnlyDistMap.values();
			clearCollection(collection);
			
			collection=null;
			isclearSta=true;
        }

        
        /*******************************************************/
		
        void clearCollection(Collection<Map<String, Map<String, String>>> collection){
        	for (Map<String, Map<String, String>> map : collection) {
				if(null!=map) {
					map.clear();
				}
			}
        }
        
	}
	
	public static void main(String[] args) {
		MailCalHandler.init();
	}
}
